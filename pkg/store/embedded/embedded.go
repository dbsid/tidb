// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build tidbone

// Package embedded runs TiKV inside the TiDB process.
//
// TiKV is linked as a static library (tikv/cmd/tikv-embed) through cgo. After
// Start, requests whose target is the local store are handed to TiKV in
// process: client-go encodes the request once as a protobuf
// BatchCommandsRequest.Request, TiKV executes it on its own thread pools and
// passes the protobuf response to a completion callback. gRPC, HTTP/2
// framing, the batch-commands stream envelope and the loopback socket are not
// involved. Requests for other stores keep using gRPC.
//
// A local call holds its OS thread for at most a short synchronous wait
// (SetSyncWait): most requests finish within it and cost one thread wake-up.
// A longer request (lock wait, large scan) is answered through a completion
// callback while the caller waits on a channel, so cancellation (KILL), the
// request timeout and the context deadline end the wait as they do for gRPC,
// and long requests do not pin threads. In-flight local calls are capped
// (SetMaxInFlight); above the cap requests use gRPC.
package embedded

/*
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

int32_t tikv_embed_start(const char *const *argv, int32_t argc);
int32_t tikv_embed_wait_ready(uint64_t timeout_ms);
uint64_t tikv_embed_store_id(void);
typedef void (*tikv_embed_done_fn)(uint64_t token, int32_t code, uint8_t *resp, size_t resp_len);
void tikv_embed_set_done(tikv_embed_done_fn done);
int32_t tikv_embed_submit(const uint8_t *req, size_t req_len, uint64_t token, uint64_t wait_us, uint8_t **resp, size_t *resp_len);
int32_t tikv_embed_health_feedback(uint8_t **out, size_t *out_len, uint64_t *interval_ms);
void tikv_embed_free(uint8_t *ptr, size_t len);
int32_t tikv_embed_stop(bool graceful, uint64_t timeout_ms);

// Implemented in Go (callback.go).
void tidbOneEmbedDone(uint64_t token, int32_t code, uint8_t *resp, size_t resp_len);
*/
import "C"

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/pkg/store/embedded/calls"
	"github.com/tikv/client-go/v2/tikv"
)

// Result codes of the C ABI, see tikv/cmd/tikv-embed/include/tikv_embed.h.
const (
	codeOK             = 0
	codeUnavailable    = 1
	codeBadRequest     = 2
	codeError          = 3
	codeTimeout        = 4
	codeExited         = 5
	codeAlreadyStarted = 6
	codePending        = 7
)

// Config describes the embedded TiKV. Fields map to tikv-server flags.
type Config struct {
	ConfigFile          string
	Addr                string
	AdvertiseAddr       string
	StatusAddr          string
	AdvertiseStatusAddr string
	DataDir             string
	LogFile             string
	Labels              string
	Capacity            string
	PDEndpoints         []string
}

// Args returns the tikv-server style arguments for cfg.
func (cfg *Config) Args() []string {
	var args []string
	add := func(name, value string) {
		if value != "" {
			args = append(args, fmt.Sprintf("--%s=%s", name, value))
		}
	}
	add("config", cfg.ConfigFile)
	add("addr", cfg.Addr)
	add("advertise-addr", cfg.AdvertiseAddr)
	add("status-addr", cfg.StatusAddr)
	add("advertise-status-addr", cfg.AdvertiseStatusAddr)
	add("data-dir", cfg.DataDir)
	add("log-file", cfg.LogFile)
	add("labels", cfg.Labels)
	add("capacity", cfg.Capacity)
	add("pd-endpoints", strings.Join(cfg.PDEndpoints, ","))
	return args
}

// DefaultMaxInFlight is the default cap on in-flight local calls. Each call
// can hold an OS thread for up to the synchronous wait, so the cap also keeps
// the thread count below Go's limit of 10,000.
const DefaultMaxInFlight = 8192

// DefaultSyncWait is how long a local call waits synchronously before it
// continues through the completion callback.
const DefaultSyncWait = time.Millisecond

// Store is the in-process TiKV. It implements tikv.LocalStore.
type Store struct {
	addr     string
	storeID  uint64
	stopped  atomic.Bool
	calls    *calls.Pending
	maxCalls atomic.Int64
	syncWait atomic.Int64 // microseconds
}

var _ tikv.LocalStore = (*Store)(nil)

// Start launches TiKV and blocks until it serves requests or readyTimeout
// passes.
func Start(cfg *Config, readyTimeout time.Duration) (*Store, error) {
	args := cfg.Args()
	cArgs := make([]*C.char, len(args))
	for i, a := range args {
		cArgs[i] = C.CString(a)
	}
	defer func() {
		for _, p := range cArgs {
			C.free(unsafe.Pointer(p))
		}
	}()
	var argv **C.char
	if len(cArgs) > 0 {
		argv = (**C.char)(C.malloc(C.size_t(len(cArgs)) * C.size_t(unsafe.Sizeof(uintptr(0)))))
		defer C.free(unsafe.Pointer(argv))
		copy(unsafe.Slice(argv, len(cArgs)), cArgs)
	}
	if rc := C.tikv_embed_start(argv, C.int32_t(len(cArgs))); rc != codeOK {
		return nil, errors.Errorf("start embedded TiKV: code %d", int(rc))
	}
	switch rc := C.tikv_embed_wait_ready(C.uint64_t(readyTimeout.Milliseconds())); rc {
	case codeOK:
	case codeTimeout:
		return nil, errors.Errorf("embedded TiKV is not ready after %s", readyTimeout)
	default:
		return nil, errors.Errorf("embedded TiKV exited during startup: code %d", int(rc))
	}
	addr := cfg.AdvertiseAddr
	if addr == "" {
		addr = cfg.Addr
	}
	C.tikv_embed_set_done(C.tikv_embed_done_fn(C.tidbOneEmbedDone))
	s := &Store{addr: addr, storeID: uint64(C.tikv_embed_store_id()), calls: pending}
	s.maxCalls.Store(DefaultMaxInFlight)
	s.SetSyncWait(DefaultSyncWait)
	return s, nil
}

// SetSyncWait sets how long a local call holds its thread waiting for TiKV
// before it continues through the completion callback.
func (s *Store) SetSyncWait(d time.Duration) { s.syncWait.Store(d.Microseconds()) }

// SetMaxInFlight caps in-flight local calls; requests above the cap use
// gRPC. n <= 0 removes the cap.
func (s *Store) SetMaxInFlight(n int64) { s.maxCalls.Store(n) }

// InFlight returns the number of local calls TiKV has not answered yet,
// including calls whose caller stopped waiting.
func (s *Store) InFlight() int64 { return s.calls.InFlight() }

// StoreID returns the ID of the local store.
func (s *Store) StoreID() uint64 { return s.storeID }

// Addr returns the advertised gRPC address of the local store.
func (s *Store) Addr() string { return s.addr }

// IsLocal implements tikv.LocalStore.
func (s *Store) IsLocal(addr string, storeID uint64) bool {
	if storeID != 0 {
		return storeID == s.storeID
	}
	return addr == s.addr
}

// Call implements tikv.LocalStore. It submits req to TiKV and waits for the
// response, ctx or timeout (0: no timeout), whichever comes first; the first
// SetSyncWait of the wait is synchronous. A call given up on keeps running in
// TiKV until it finishes, as a gRPC request does; its response is dropped.
func (s *Store) Call(ctx context.Context, req []byte, resp *tikvpb.BatchCommandsResponse_Response, timeout time.Duration) error {
	if s.stopped.Load() {
		return tikv.ErrLocalStoreUnavailable
	}
	if max := s.maxCalls.Load(); max > 0 && s.calls.InFlight() >= max {
		return tikv.ErrLocalStoreUnavailable
	}
	token, ch := s.calls.Add()
	var reqPtr *C.uint8_t
	if len(req) > 0 {
		reqPtr = (*C.uint8_t)(unsafe.Pointer(&req[0]))
	}
	var (
		out    *C.uint8_t
		outLen C.size_t
	)
	rc := C.tikv_embed_submit(reqPtr, C.size_t(len(req)), C.uint64_t(token), C.uint64_t(s.syncWait.Load()), &out, &outLen)
	switch rc {
	case codePending:
	case codeOK, codeError:
		// Answered within the synchronous wait; no callback follows.
		s.calls.NoCallback(token)
		var data []byte
		if out != nil {
			// Copy into Go memory: kvproto decodes some byte fields
			// (sharedbytes.SharedBytes) without copying.
			data = C.GoBytes(unsafe.Pointer(out), C.int(outLen))
			C.tikv_embed_free(out, outLen)
		}
		if rc != codeOK {
			return errors.Errorf("embedded TiKV call failed (code %d): %s", int(rc), data)
		}
		return resp.Unmarshal(data)
	case codeUnavailable:
		s.calls.NoCallback(token)
		return tikv.ErrLocalStoreUnavailable
	default:
		s.calls.NoCallback(token)
		return errors.Errorf("embedded TiKV rejected the request (code %d)", int(rc))
	}
	var timer <-chan time.Time
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		timer = t.C
	}
	select {
	case r := <-ch:
		if r.Code != codeOK {
			return errors.Errorf("embedded TiKV call failed (code %d): %s", int(r.Code), r.Data)
		}
		// r.Data is Go memory: kvproto decodes some byte fields
		// (sharedbytes.SharedBytes, e.g. coprocessor data) without copying.
		return resp.Unmarshal(r.Data)
	case <-ctx.Done():
		s.calls.Abandon(token)
		return errors.WithStack(ctx.Err())
	case <-timer:
		s.calls.Abandon(token)
		return errors.WithMessage(context.DeadlineExceeded, "wait embedded TiKV")
	}
}

// HealthFeedback returns the local store's health feedback and how long to
// wait before asking again. It implements the optional health feedback
// interface of tikv.LocalStore; the gRPC batch-commands stream delivers the
// same feedback for remote stores.
func (s *Store) HealthFeedback() (*kvrpcpb.HealthFeedback, time.Duration, error) {
	if s.stopped.Load() {
		return nil, 0, tikv.ErrLocalStoreUnavailable
	}
	var (
		out      *C.uint8_t
		outLen   C.size_t
		interval C.uint64_t
	)
	rc := C.tikv_embed_health_feedback(&out, &outLen, &interval)
	if out != nil {
		defer C.tikv_embed_free(out, outLen)
	}
	if rc != codeOK {
		return nil, 0, tikv.ErrLocalStoreUnavailable
	}
	fb := &kvrpcpb.HealthFeedback{}
	if out != nil && outLen > 0 {
		if err := fb.Unmarshal(C.GoBytes(unsafe.Pointer(out), C.int(outLen))); err != nil {
			return nil, 0, err
		}
	}
	return fb, time.Duration(interval) * time.Millisecond, nil
}

// Stop shuts TiKV down. Requests sent afterwards fall back to gRPC.
func (s *Store) Stop(graceful bool, timeout time.Duration) error {
	s.stopped.Store(true)
	if rc := C.tikv_embed_stop(C.bool(graceful), C.uint64_t(timeout.Milliseconds())); rc != codeOK {
		return errors.Errorf("stop embedded TiKV: code %d", int(rc))
	}
	return nil
}

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
// BatchCommandsRequest.Request, TiKV executes it on its own thread pools, and
// the protobuf response is decoded directly from TiKV's buffer. gRPC, HTTP/2
// framing, the batch-commands stream envelope and the loopback socket are not
// involved. Requests for other stores keep using gRPC.
package embedded

/*
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

int32_t tikv_embed_start(const char *const *argv, int32_t argc);
int32_t tikv_embed_wait_ready(uint64_t timeout_ms);
uint64_t tikv_embed_store_id(void);
int32_t tikv_embed_call(const uint8_t *req, size_t req_len, uint8_t **resp, size_t *resp_len);
void tikv_embed_free(uint8_t *ptr, size_t len);
int32_t tikv_embed_stop(bool graceful, uint64_t timeout_ms);
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
	"github.com/pingcap/kvproto/pkg/tikvpb"
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

// Store is the in-process TiKV. It implements tikv.LocalStore.
type Store struct {
	addr    string
	storeID uint64
	stopped atomic.Bool
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
	return &Store{addr: addr, storeID: uint64(C.tikv_embed_store_id())}, nil
}

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

// Call implements tikv.LocalStore. It blocks the calling goroutine's thread
// until TiKV answers; the work itself runs on TiKV's thread pools.
func (s *Store) Call(_ context.Context, req []byte, resp *tikvpb.BatchCommandsResponse_Response, _ time.Duration) error {
	if s.stopped.Load() {
		return tikv.ErrLocalStoreUnavailable
	}
	var (
		reqPtr *C.uint8_t
		out    *C.uint8_t
		outLen C.size_t
	)
	if len(req) > 0 {
		reqPtr = (*C.uint8_t)(unsafe.Pointer(&req[0]))
	}
	rc := C.tikv_embed_call(reqPtr, C.size_t(len(req)), &out, &outLen)
	if out != nil {
		defer C.tikv_embed_free(out, outLen)
	}
	switch rc {
	case codeOK:
		// Copy into Go memory before decoding: kvproto decodes some byte
		// fields (sharedbytes.SharedBytes, e.g. coprocessor data) without
		// copying, so the decoded response may alias the buffer, and TiKV's
		// buffer is freed when Call returns.
		var buf []byte
		if out != nil && outLen > 0 {
			buf = C.GoBytes(unsafe.Pointer(out), C.int(outLen))
		}
		return resp.Unmarshal(buf)
	case codeUnavailable:
		return tikv.ErrLocalStoreUnavailable
	default:
		var msg string
		if out != nil && outLen > 0 {
			msg = C.GoStringN((*C.char)(unsafe.Pointer(out)), C.int(outLen))
		}
		return errors.Errorf("embedded TiKV call failed (code %d): %s", int(rc), msg)
	}
}

// Stop shuts TiKV down. Requests sent afterwards fall back to gRPC.
func (s *Store) Stop(graceful bool, timeout time.Duration) error {
	s.stopped.Store(true)
	if rc := C.tikv_embed_stop(C.bool(graceful), C.uint64_t(timeout.Milliseconds())); rc != codeOK {
		return errors.Errorf("stop embedded TiKV: code %d", int(rc))
	}
	return nil
}

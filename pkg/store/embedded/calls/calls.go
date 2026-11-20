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

// Package calls tracks the local calls submitted to the embedded TiKV until
// its completion callback answers them.
package calls

import (
	"sync"
	"sync/atomic"
)

// Result is what the completion callback delivers for one call.
type Result struct {
	Code int32
	Data []byte
}

const shards = 64

// Pending maps the token of every submitted call to the channel its caller
// waits on. The callback runs on TiKV threads, so lookups are sharded.
type Pending struct {
	next     atomic.Uint64
	inFlight atomic.Int64
	shards   [shards]shard
}

type shard struct {
	mu      sync.Mutex
	waiters map[uint64]chan Result
}

func (p *Pending) shard(token uint64) *shard { return &p.shards[token%shards] }

// Add registers a call before it is submitted and returns its token and the
// channel that receives its result.
func (p *Pending) Add() (uint64, <-chan Result) {
	token := p.next.Add(1)
	ch := make(chan Result, 1)
	s := p.shard(token)
	s.mu.Lock()
	if s.waiters == nil {
		s.waiters = make(map[uint64]chan Result)
	}
	s.waiters[token] = ch
	s.mu.Unlock()
	p.inFlight.Add(1)
	return token, ch
}

func (p *Pending) remove(token uint64) chan Result {
	s := p.shard(token)
	s.mu.Lock()
	ch, ok := s.waiters[token]
	if ok {
		delete(s.waiters, token)
	}
	s.mu.Unlock()
	return ch
}

// Unsubmitted removes a call TiKV did not accept; no callback will come.
func (p *Pending) Unsubmitted(token uint64) {
	if p.remove(token) != nil {
		p.inFlight.Add(-1)
	}
}

// Abandon removes the waiter of a submitted call whose caller stopped
// waiting. The call stays in flight until its callback arrives.
func (p *Pending) Abandon(token uint64) { p.remove(token) }

// Take is called once by the completion callback of a submitted call. It
// ends the call and returns the waiting channel, or nil if the caller gave
// up; the callback then skips copying the response.
func (p *Pending) Take(token uint64) chan<- Result {
	p.inFlight.Add(-1)
	if ch := p.remove(token); ch != nil {
		return ch
	}
	return nil
}

// InFlight returns the number of submitted calls without a callback yet, and
// of registered calls not submitted yet.
func (p *Pending) InFlight() int64 { return p.inFlight.Load() }

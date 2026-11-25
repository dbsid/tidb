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

package calls

import (
	"sync"
	"time"
)

// Without a synchronous wait every local call waits on its channel together
// with its timeout, so the timers are reused instead of allocated per call.
//
// Reuse relies on the synchronous timer channels of Go 1.23 and later (the
// only behavior since Go 1.27 removed GODEBUG asynctimerchan; go.mod requires
// go 1.27): once Stop or Reset returns, no expiry from before the call is
// received, so a reused timer never delivers a stale value.
var timers = sync.Pool{New: func() any {
	t := time.NewTimer(time.Hour)
	t.Stop()
	return t
}}

// GetTimer returns a stopped timer reset to fire after d. Return it with
// PutTimer.
func GetTimer(d time.Duration) *time.Timer {
	t := timers.Get().(*time.Timer)
	t.Reset(d)
	return t
}

// PutTimer stops t and keeps it for reuse; the caller must not use t
// afterwards. t may have fired, whether or not its value was received.
func PutTimer(t *time.Timer) {
	t.Stop()
	timers.Put(t)
}

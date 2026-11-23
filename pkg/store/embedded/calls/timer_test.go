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
	"testing"
	"time"
)

func TestTimerFires(t *testing.T) {
	tm := GetTimer(time.Millisecond)
	defer PutTimer(tm)
	select {
	case <-tm.C:
	case <-time.After(10 * time.Second):
		t.Fatal("timer did not fire")
	}
}

// A timer returned after it fired, with its value never received, must not
// deliver that stale value to the next user.
func TestTimerReuseNoStaleExpiry(t *testing.T) {
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				fired := GetTimer(time.Nanosecond)
				time.Sleep(20 * time.Microsecond)
				PutTimer(fired)

				tm := GetTimer(time.Hour)
				select {
				case <-tm.C:
					t.Error("reused timer delivered a stale expiry")
				case <-time.After(50 * time.Microsecond):
				}
				PutTimer(tm)
			}
		}()
	}
	wg.Wait()
}

func BenchmarkNewTimer(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tm := time.NewTimer(30 * time.Second)
			tm.Stop()
		}
	})
}

func BenchmarkGetPutTimer(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			PutTimer(GetTimer(30 * time.Second))
		}
	})
}

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

	"github.com/stretchr/testify/require"
)

func TestPending(t *testing.T) {
	var p Pending

	// Answered call.
	tok, ch := p.Add()
	require.EqualValues(t, 1, p.InFlight())
	w := p.Take(tok)
	require.NotNil(t, w)
	w <- Result{Code: 0, Data: []byte("x")}
	require.Equal(t, []byte("x"), (<-ch).Data)
	require.EqualValues(t, 0, p.InFlight())

	// Not accepted by TiKV: no callback follows.
	tok, _ = p.Add()
	p.Unsubmitted(tok)
	require.EqualValues(t, 0, p.InFlight())

	// Abandoned (cancelled, timed out): in flight until the callback.
	tok, _ = p.Add()
	p.Abandon(tok)
	require.EqualValues(t, 1, p.InFlight())
	require.Nil(t, p.Take(tok))
	require.EqualValues(t, 0, p.InFlight())
}

func TestPendingConcurrent(t *testing.T) {
	var p Pending
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				tok, ch := p.Add()
				go func() { p.Take(tok) <- Result{} }()
				<-ch
			}
		}()
	}
	wg.Wait()
	require.EqualValues(t, 0, p.InFlight())
}

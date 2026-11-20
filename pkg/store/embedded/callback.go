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

package embedded

/*
#include <stddef.h>
#include <stdint.h>

void tikv_embed_free(uint8_t *ptr, size_t len);
*/
import "C"

import (
	"unsafe"

	"github.com/pingcap/tidb/pkg/store/embedded/calls"
)

// pending holds the local calls of the process; there is one embedded TiKV.
var pending = &calls.Pending{}

// tidbOneEmbedDone is the completion callback of tikv_embed_submit. It runs on
// a TiKV thread: it copies the response into Go memory, frees TiKV's buffer
// and hands the copy to the waiting caller without blocking.
//
//export tidbOneEmbedDone
func tidbOneEmbedDone(token C.uint64_t, code C.int32_t, resp *C.uint8_t, respLen C.size_t) {
	ch := pending.Take(uint64(token))
	if ch != nil {
		var data []byte
		if resp != nil && respLen > 0 {
			data = C.GoBytes(unsafe.Pointer(resp), C.int(respLen))
		}
		ch <- calls.Result{Code: int32(code), Data: data}
	}
	if resp != nil {
		C.tikv_embed_free(resp, respLen)
	}
}

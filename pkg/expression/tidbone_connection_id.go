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

package expression

import (
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/tidbone/sessiontrack"
)

// tidbOneConnectionID is CONNECTION_ID() of a session: the connection ID the
// client of a locality-router session saw at login, the same on every server
// the session runs on. Other sessions get their own connection ID.
func tidbOneConnectionID(vars *variable.SessionVars) uint64 {
	return sessiontrack.ConnectionID(vars)
}

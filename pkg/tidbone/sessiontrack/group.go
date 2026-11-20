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

package sessiontrack

import (
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

// SessionGroupVar names the session variable that ties the backend
// connections of one locality-router session together.
//
// The router opens one connection per server for a client session and sets
// the variable on each of them to the connection ID the client saw in the
// handshake. With it set, CONNECTION_ID() returns that ID on every
// connection of the session, and `KILL [QUERY] id` reaches the connection
// that runs the session wherever it is, also when the KILL comes through
// another router process. The variable is registered by tidb-one builds
// only (see EnableGroups).
const SessionGroupVar = "tidb_one_session_group"

var (
	groupsEnabled atomic.Bool
	// groups maps a connection ID to its session group. It is read by KILL
	// from other goroutines, so it does not use the session variables.
	groups sync.Map
)

// EnableGroups turns on session groups; tidb-one builds call it when they
// register SessionGroupVar.
func EnableGroups() { groupsEnabled.Store(true) }

// GroupsEnabled reports whether session groups are on.
func GroupsEnabled() bool { return groupsEnabled.Load() }

// SetConnGroup records the session group of a connection; group 0 removes it.
func SetConnGroup(connID, group uint64) {
	if connID == 0 {
		return
	}
	if group == 0 {
		groups.Delete(connID)
		return
	}
	groups.Store(connID, group)
}

// ForgetConn removes a closed connection.
func ForgetConn(connID uint64) {
	if connID != 0 {
		groups.Delete(connID)
	}
}

// GroupMembers returns the connections of this server in group, except the
// connection whose ID is the group ID itself (KILL handles it already).
func GroupMembers(group uint64) []uint64 {
	if group == 0 {
		return nil
	}
	var ids []uint64
	groups.Range(func(k, v any) bool {
		if id := k.(uint64); id != group && v.(uint64) == group {
			ids = append(ids, id)
		}
		return true
	})
	return ids
}

// ConnectionID is the value of CONNECTION_ID() for a session: its session
// group if one is set, otherwise its own connection ID.
func ConnectionID(vars *variable.SessionVars) uint64 {
	if v, ok := vars.GetSystemVar(SessionGroupVar); ok {
		if g, err := strconv.ParseUint(v, 10, 64); err == nil && g != 0 {
			return g
		}
	}
	return vars.ConnectionID
}

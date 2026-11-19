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

package session

import (
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
)

// TiDBOneMigrationBlocked reports whether the session holds state that
// `SHOW SESSION_STATES` / `SET SESSION_STATES` cannot move to another
// server. It is the transaction-independent part of CheckCanMigrateStates
// (a router only moves sessions between transactions, and while a result set
// of an autocommit statement is being written the transaction is still open),
// plus a pending `SET TRANSACTION ...`. The session states carry that
// one-shot setting, but every statement that ends advances it (set -> use ->
// gone), so any statement the router runs on the new server before the
// transaction (catching up LAST_INSERT_ID(), for example) would consume it.
//
// tidb-one's server package reports it to the locality router in the server
// status of every OK / EOF packet (see pkg/server/tidbone_session_track.go).
func (s *session) TiDBOneMigrationBlocked() bool {
	vars := s.sessionVars
	if vars.LocalTemporaryTables != nil {
		if tables, ok := vars.LocalTemporaryTables.(*infoschema.SessionTables); ok && tables.Count() > 0 {
			return true
		}
	}
	if len(s.advisoryLocks) > 0 || len(s.lockedTables) > 0 || s.InSandBoxMode() {
		return true
	}
	_, pending, err := vars.GetSessionStatesSystemVar(vardef.TxnIsolationOneShot)
	return err == nil && pending
}

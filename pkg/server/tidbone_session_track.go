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

package server

import (
	"math"
	"strconv"

	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/tidbone/sessiontrack"
)

// tidb-one session tracking for the TiProxy locality-router.
//
// A router that moves a session between servers at transaction boundaries
// enables it with `SET SESSION tidb_one_session_track = ON`. The setting is
// part of the session states, so it follows the session when it migrates.
// While it is on, the server status of every OK and EOF packet carries:
//
//   - TiDBOneStatusStateChanged (0x4000, SERVER_SESSION_STATE_CHANGED): the
//     statement may have changed state that `SHOW SESSION_STATES` carries;
//   - TiDBOneStatusLastInsertID (0x8000): the statement set LAST_INSERT_ID();
//     an OK packet then holds the new value in its last-insert-id field;
//   - TiDBOneStatusUnmigratable (0x0004): the session holds state that cannot
//     migrate safely (local temporary tables, advisory locks, LOCK TABLES,
//     sandbox mode, a pending SET TRANSACTION).
//
// The router removes these bits before it forwards a packet to its client.
const (
	TiDBOneSessionTrack       = "tidb_one_session_track"
	TiDBOneStatusUnmigratable = uint16(0x0004)
	TiDBOneStatusStateChanged = uint16(0x4000)
	TiDBOneStatusLastInsertID = uint16(0x8000)
	tidbOneStatusMask         = TiDBOneStatusUnmigratable | TiDBOneStatusStateChanged | TiDBOneStatusLastInsertID
)

func init() {
	variable.RegisterSysVar(&variable.SysVar{
		Scope: vardef.ScopeSession,
		Name:  TiDBOneSessionTrack,
		Value: vardef.Off,
		Type:  vardef.TypeBool,
	})
	// The router sets the session group on each backend connection of a
	// client session; see sessiontrack.SessionGroupVar.
	variable.RegisterSysVar(&variable.SysVar{
		Scope:    vardef.ScopeSession,
		Name:     sessiontrack.SessionGroupVar,
		Value:    "0",
		Type:     vardef.TypeUnsigned,
		MinValue: 0,
		MaxValue: math.MaxUint64,
		SetSession: func(s *variable.SessionVars, val string) error {
			g, err := strconv.ParseUint(val, 10, 64)
			if err != nil {
				return err
			}
			sessiontrack.SetConnGroup(s.ConnectionID, g)
			return nil
		},
	})
	sessiontrack.EnableGroups()
}

// tidbOneForgetConn drops a closed connection from its session group.
func (cc *clientConn) tidbOneForgetConn() {
	sessiontrack.ForgetConn(cc.connectionID)
}

func tidbOneTracking(vars *variable.SessionVars) bool {
	v, ok := vars.GetSystemVar(TiDBOneSessionTrack)
	return ok && v == vardef.On
}

// tidbOneBeginStmt records, before stmt runs, whether it may change the
// migratable session state. The flag is kept in the session status so that
// every OK / EOF packet of the statement reports it.
func (cc *clientConn) tidbOneBeginStmt(stmt ast.StmtNode) {
	vars := cc.ctx.GetSessionVars()
	changed := tidbOneTracking(vars) && sessiontrack.MayChangeSessionState(stmt)
	if changed || vars.Status()&TiDBOneStatusStateChanged != 0 {
		vars.SetStatusFlag(TiDBOneStatusStateChanged, changed)
	}
}

// tidbOneBeginExecute is tidbOneBeginStmt for a binary-protocol execution of
// prepStmt; it inspects the prepared statement, not the EXECUTE wrapper.
func (cc *clientConn) tidbOneBeginExecute(prepStmt any, execStmt *ast.ExecuteStmt) {
	if p, ok := prepStmt.(*plannercore.PlanCacheStmt); ok && p.PreparedAst != nil {
		cc.tidbOneBeginStmt(p.PreparedAst.Stmt)
		return
	}
	cc.tidbOneBeginStmt(execStmt)
}

// tidbOneStatus adds the per-packet tracking bits to a server status.
func (cc *clientConn) tidbOneStatus(status uint16) uint16 {
	vars := cc.ctx.GetSessionVars()
	if !tidbOneTracking(vars) {
		return status &^ tidbOneStatusMask
	}
	if vars.StmtCtx != nil && vars.StmtCtx.LastInsertID > 0 {
		status |= TiDBOneStatusLastInsertID
	}
	if b, ok := cc.ctx.Session.(interface{ TiDBOneMigrationBlocked() bool }); ok && b.TiDBOneMigrationBlocked() {
		status |= TiDBOneStatusUnmigratable
	}
	return status
}

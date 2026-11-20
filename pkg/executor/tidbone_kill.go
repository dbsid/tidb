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

package executor

import (
	"context"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/tidbone/sessiontrack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// tidb-one session groups (see sessiontrack.SessionGroupVar): a
// locality-router session has one connection per server, all in the group
// whose ID is the connection ID its client knows. `KILL [QUERY] id` also
// kills the connections of group id, on every server, so it stops the
// session wherever it runs and whichever router or server receives it.
//
// Authorization follows the target ID as for any global KILL: the planner
// checks the owner of connection id when it is on this server.

// tidbOneKillGroupLocal kills the connections of group on this server.
func tidbOneKillGroupLocal(sm sessmgr.Manager, group uint64, query bool, fromRemote bool) {
	if !sessiontrack.GroupsEnabled() {
		return
	}
	for _, id := range sessiontrack.GroupMembers(group) {
		killBySQLStmt(sm, id, query, fromRemote)
	}
}

// tidbOneKillGroup kills the connections of group on every server. The
// server of connection group itself (targetServer) already received the KILL
// and kills the group there.
func (e *SimpleExec) tidbOneKillGroup(ctx context.Context, sm sessmgr.Manager, group, targetServer uint64, query bool) {
	if !sessiontrack.GroupsEnabled() {
		return
	}
	tidbOneKillGroupLocal(sm, group, query, false)
	servers, err := infosync.GetAllServerInfo(ctx)
	if err != nil {
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("KILL session group: " + err.Error()))
		return
	}
	self := sm.ServerID()
	for _, info := range servers {
		if info.ServerIDGetter == nil {
			continue
		}
		id := info.ServerIDGetter()
		if id == 0 || id == self || id == targetServer {
			continue
		}
		if err := tidbOneKillOnServer(ctx, e.Ctx(), id, group, query); err != nil {
			e.Ctx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("KILL session group on server " + info.ID + ": " + err.Error()))
		}
	}
}

// tidbOneKillOnServer sends `KILL [QUERY] connID` to the server serverID,
// which need not own connID; it kills the session group connID there. It is
// killRemoteConn with the server and the connection given separately.
func tidbOneKillOnServer(ctx context.Context, sctx sessionctx.Context, serverID, connID uint64, query bool) error {
	dagReq := &tipb.DAGRequest{}
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(sctx.GetSessionVars().Location())
	dagReq.Flags = sctx.GetSessionVars().StmtCtx.PushDownFlags()
	dagReq.Executors = []*tipb.Executor{{
		Tp:   tipb.ExecType_TypeKill,
		Kill: &tipb.Kill{ConnID: connID, Query: query},
	}}
	var builder distsql.RequestBuilder
	kvReq, err := builder.
		SetDAGRequest(dagReq).
		SetFromSessionVars(sctx.GetDistSQLCtx()).
		SetFromInfoSchema(sctx.GetInfoSchema()).
		SetStoreType(kv.TiDB).
		SetTiDBServerID(serverID).
		SetStartTS(math.MaxUint64).
		Build()
	if err != nil {
		return err
	}
	resp := sctx.GetClient().Send(ctx, kvReq, sctx.GetSessionVars().KVVars, &kv.ClientSendOption{})
	if resp == nil {
		return errors.New("client returns nil response")
	}
	defer func() {
		_ = resp.Close()
	}()
	if _, err := resp.Next(ctx); err != nil {
		return errors.Trace(err)
	}
	logutil.BgLogger().Info("killed session group on remote server", zap.Uint64("serverID", serverID),
		zap.Uint64("group", connID), zap.Bool("query", query))
	return nil
}

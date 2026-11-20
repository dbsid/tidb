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
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/tidbone/sessiontrack"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/stretchr/testify/require"
)

func TestTiDBOneSessionGroup(t *testing.T) {
	const group = 9000001
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	id1 := tk1.Session().GetSessionVars().ConnectionID

	tk1.MustQuery("select connection_id()").Check(testkit.Rows(sqlescape.MustEscapeSQL("%?", id1)))
	tk1.MustExec("set session tidb_one_session_group = 9000001")
	tk1.MustQuery("select connection_id()").Check(testkit.Rows("9000001"))
	// Vectorized evaluation.
	tk1.MustExec("create table t (a int)")
	tk1.MustExec("insert into t values (1), (2), (3)")
	tk1.MustQuery("select connection_id() from t").Check(testkit.Rows("9000001", "9000001", "9000001"))
	require.Contains(t, sessiontrack.GroupMembers(group), id1)

	// The group follows the session states to another connection, as when
	// the router opens a connection on another server.
	states := tk1.MustQuery("show session_states").Rows()[0][0].(string)
	tk2 := testkit.NewTestKit(t, store)
	id2 := tk2.Session().GetSessionVars().ConnectionID
	tk2.MustExec(sqlescape.MustEscapeSQL("set session_states %?", states))
	tk2.MustQuery("select connection_id()").Check(testkit.Rows("9000001"))
	require.ElementsMatch(t, []uint64{id1, id2}, sessiontrack.GroupMembers(group))

	// Leaving the group, and closing a connection.
	tk1.MustExec("set session tidb_one_session_group = 0")
	tk1.MustQuery("select connection_id()").Check(testkit.Rows(sqlescape.MustEscapeSQL("%?", id1)))
	(&clientConn{connectionID: id2}).tidbOneForgetConn()
	require.Empty(t, sessiontrack.GroupMembers(group))
}

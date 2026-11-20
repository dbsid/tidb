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
	"math"
	"slices"
	"testing"

	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

func TestGroupMembers(t *testing.T) {
	SetConnGroup(10, 10)
	SetConnGroup(11, 10)
	SetConnGroup(12, 10)
	SetConnGroup(20, 20)
	SetConnGroup(0, 10) // internal sessions are ignored
	defer func() {
		for _, id := range []uint64{10, 11, 12, 20} {
			ForgetConn(id)
		}
	}()

	got := GroupMembers(10)
	slices.Sort(got)
	require.Equal(t, []uint64{11, 12}, got, "the group's own ID is excluded")
	require.Empty(t, GroupMembers(20))
	require.Empty(t, GroupMembers(0))

	ForgetConn(11)
	SetConnGroup(12, 0)
	require.Empty(t, GroupMembers(10))
}

func TestConnectionID(t *testing.T) {
	variable.RegisterSysVar(&variable.SysVar{
		Scope: vardef.ScopeSession, Name: SessionGroupVar, Value: "0",
		Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint64,
	})
	defer variable.UnregisterSysVar(SessionGroupVar)

	vars := variable.NewSessionVars(nil)
	vars.ConnectionID = 7
	require.EqualValues(t, 7, ConnectionID(vars))
	require.NoError(t, vars.SetSystemVar(SessionGroupVar, "42"))
	require.EqualValues(t, 42, ConnectionID(vars))
	require.NoError(t, vars.SetSystemVar(SessionGroupVar, "0"))
	require.EqualValues(t, 7, ConnectionID(vars))
}

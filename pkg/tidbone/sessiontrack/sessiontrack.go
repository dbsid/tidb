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

// Package sessiontrack decides whether a statement can change the session
// state that `SHOW SESSION_STATES` carries. tidb-one uses it to tell its
// transaction router (TiProxy locality-router) when a session must be
// re-synchronized before it moves to another server.
//
// The check is syntactic and conservative: it may report a change that does
// not happen (the router then performs one extra synchronization), but it must
// not miss one.
package sessiontrack

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// MayChangeSessionState reports whether executing stmt can change migratable
// session state: system or user variables, the current database, text-protocol
// prepared statements, session bindings, sequence values, roles, the resource
// group, hypothetical indexes or LAST_INSERT_ID() set by an expression.
//
// Values that only describe the previous statement (affected rows, found rows,
// warnings) and auto-increment IDs reported in the OK packet are not covered;
// the router handles them itself.
func MayChangeSessionState(stmt ast.StmtNode) bool {
	switch s := stmt.(type) {
	case nil:
		return true
	case *ast.SetStmt, *ast.UseStmt, *ast.PrepareStmt, *ast.DeallocateStmt,
		*ast.SetSessionStatesStmt, *ast.SetRoleStmt, *ast.SetResourceGroupStmt,
		*ast.ExecuteStmt,
		// DDL is rare; these can create or drop hypothetical indexes or
		// hypothetical TiFlash replicas, which are session state.
		*ast.CreateIndexStmt, *ast.DropIndexStmt, *ast.AlterTableStmt:
		return true
	case *ast.CreateBindingStmt:
		return !s.GlobalScope
	case *ast.DropBindingStmt:
		return !s.GlobalScope
	}
	v := &visitor{}
	stmt.Accept(v)
	return v.changes
}

type visitor struct {
	changes bool
}

func (v *visitor) Enter(n ast.Node) (ast.Node, bool) {
	if v.changes {
		return n, true
	}
	switch x := n.(type) {
	case *ast.VariableExpr:
		// `@a := expr` assigns a user variable.
		if x.Value != nil {
			v.changes = true
		}
	case *ast.FuncCallExpr:
		switch x.FnName.L {
		case ast.NextVal, ast.SetVal, ast.GetLock, ast.ReleaseLock, ast.ReleaseAllLocks:
			v.changes = true
		case ast.LastInsertId:
			// LAST_INSERT_ID(expr) sets the value; LAST_INSERT_ID() reads it.
			if len(x.Args) > 0 {
				v.changes = true
			}
		}
	case *ast.SelectStmt:
		if x.SelectIntoOpt != nil && x.SelectIntoOpt.Tp == ast.SelectIntoVars {
			v.changes = true
		}
	}
	return n, v.changes
}

func (*visitor) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}

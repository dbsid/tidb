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
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/stretchr/testify/require"
)

func TestMayChangeSessionState(t *testing.T) {
	p := parser.New()
	cases := []struct {
		sql     string
		changes bool
	}{
		{"SELECT c FROM t WHERE id = 1", false},
		{"SELECT LAST_INSERT_ID()", false},
		{"INSERT INTO t (a) VALUES (1)", false},
		{"UPDATE t SET c = c + 1 WHERE id = ?", false},
		{"DELETE FROM t WHERE id = 1", false},
		{"BEGIN", false},
		{"COMMIT", false},
		{"SHOW SESSION_STATES", false},
		{"SELECT ROW_COUNT(), FOUND_ROWS()", false},
		{"SELECT LASTVAL(s)", false},
		{"SET @a = 1", true},
		{"SET SESSION sql_mode = ''", true},
		{"SET NAMES utf8mb4", true},
		{"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", true},
		{"USE test", true},
		{"PREPARE s FROM 'SELECT 1'", true},
		{"DEALLOCATE PREPARE s", true},
		{"EXECUTE s", true},
		{"SET ROLE ALL", true},
		{"SET RESOURCE GROUP rg1", true},
		{"SET SESSION_STATES '{}'", true},
		{"CREATE SESSION BINDING FOR SELECT * FROM t USING SELECT * FROM t", true},
		{"CREATE GLOBAL BINDING FOR SELECT * FROM t USING SELECT * FROM t", false},
		{"SELECT @a := c FROM t WHERE id = 1", true},
		{"SELECT NEXTVAL(s)", true},
		{"SELECT NEXT VALUE FOR s", true},
		{"SELECT SETVAL(s, 10)", true},
		{"SELECT LAST_INSERT_ID(5)", true},
		{"INSERT INTO t (id) VALUES (LAST_INSERT_ID(7))", true},
		{"SELECT GET_LOCK('a', 1)", true},
		{"DO RELEASE_LOCK('a')", true},
		{"SELECT * FROM t WHERE id IN (SELECT @b := 1)", true},
		{"CREATE INDEX idx TYPE HYPO ON t (a)", true},
	}
	for _, c := range cases {
		stmt, err := p.ParseOneStmt(c.sql, "", "")
		require.NoError(t, err, c.sql)
		require.Equal(t, c.changes, MayChangeSessionState(stmt), c.sql)
	}
	require.True(t, MayChangeSessionState(nil))
}

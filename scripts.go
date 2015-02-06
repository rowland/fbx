package fbx

import (
	"database/sql"
	"strings"
)

func ExecScript(db *sql.DB, script string) (err error) {
	// TODO: handle "set term"
	stmts := strings.Split(script, ";")
	for _, stmt := range stmts {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		_, err = db.Exec(stmt)
		if err != nil {
			return
		}
	}
	return
}

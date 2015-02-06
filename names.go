package fbx

import (
	"database/sql"
	"strings"
	"unicode"
)

func SequenceNames(db *sql.DB) (names []string, err error) {
	const query = `SELECT RDB$GENERATOR_NAME FROM RDB$GENERATORS 
		WHERE (RDB$SYSTEM_FLAG IS NULL OR RDB$SYSTEM_FLAG <> 1) 
		ORDER BY RDB$GENERATOR_NAME`
	return queryNames(db, query)
}

func TableNames(db *sql.DB) (names []string, err error) {
	const query = `SELECT RDB$RELATION_NAME FROM RDB$RELATIONS 
		WHERE (RDB$SYSTEM_FLAG <> 1 OR RDB$SYSTEM_FLAG IS NULL) AND RDB$VIEW_BLR IS NULL 
		ORDER BY RDB$RELATION_NAME`
	return queryNames(db, query)
}

func queryNames(db *sql.DB, query string, args ...interface{}) (names []string, err error) {
	rows, err := db.Query(query)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return
		}
		name = strings.TrimRightFunc(name, unicode.IsSpace)
		names = append(names, name)
	}
	err = rows.Err()
	return
}

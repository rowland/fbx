package fbx

import (
	"database/sql"
	"strings"
	"unicode"
)

func ColumnNames(db *sql.DB, tableName string) (names []string, err error) {
	cols, err := Columns(db, tableName)
	if err != nil {
		return
	}
	for _, col := range cols {
		names = append(names, col.Name)
	}
	return
}

func IndexColumnNames(db *sql.DB, indexName string) (names []string, err error) {
	const query = `SELECT RDB$FIELD_NAME
		FROM RDB$INDEX_SEGMENTS 
		WHERE RDB$INDEX_SEGMENTS.RDB$INDEX_NAME = ? 
		ORDER BY RDB$INDEX_SEGMENTS.RDB$FIELD_POSITION`
	return queryNames(db, query, indexName)
}

func PrimaryKey(db *sql.DB, tableName string) (names []string, err error) {
	const query = `
		SELECT S.RDB$FIELD_NAME
		FROM RDB$INDICES I
			JOIN RDB$INDEX_SEGMENTS S ON I.RDB$INDEX_NAME = S.RDB$INDEX_NAME
			LEFT JOIN RDB$RELATION_CONSTRAINTS C ON I.RDB$INDEX_NAME = C.RDB$INDEX_NAME
		WHERE I.RDB$RELATION_NAME = ? AND C.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY'
		ORDER BY RDB$FIELD_POSITION;`
	return queryNames(db, query, tableName)
}

func ProcedureNames(db *sql.DB) (names []string, err error) {
	const query = "SELECT RDB$PROCEDURE_NAME FROM RDB$PROCEDURES ORDER BY RDB$PROCEDURE_NAME"
	return queryNames(db, query)
}

func RoleNames(db *sql.DB) (names []string, err error) {
	const query = "SELECT RDB$ROLE_NAME FROM RDB$ROLES WHERE RDB$SYSTEM_FLAG = 0 ORDER BY RDB$ROLE_NAME"
	return queryNames(db, query)
}

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

func TriggerNames(db *sql.DB) (names []string, err error) {
	const query = "SELECT RDB$TRIGGER_NAME FROM RDB$TRIGGERS WHERE RDB$SYSTEM_FLAG = 0 ORDER BY RDB$TRIGGER_NAME"
	return queryNames(db, query)
}

func ViewNames(db *sql.DB) (names []string, err error) {
	const query = `SELECT RDB$RELATION_NAME FROM RDB$RELATIONS 
		WHERE (RDB$SYSTEM_FLAG <> 1 OR RDB$SYSTEM_FLAG IS NULL) AND NOT RDB$VIEW_BLR IS NULL AND RDB$FLAGS = 1 
		ORDER BY RDB$RELATION_NAME`
	return queryNames(db, query)
}

func queryNames(db *sql.DB, query string, args ...interface{}) (names []string, err error) {
	rows, err := db.Query(query, args...)
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

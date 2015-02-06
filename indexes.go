package fbx

import (
	"database/sql"
	"strings"
	"unicode"
)

type Index struct {
	Name       string
	TableName  string
	Unique     sql.NullBool
	Descending sql.NullBool
	Columns    []string
}

func Indexes(db *sql.DB) (indexes []*Index, err error) {
	const query = `
		SELECT RDB$INDICES.RDB$RELATION_NAME, RDB$INDICES.RDB$INDEX_NAME, RDB$INDICES.RDB$UNIQUE_FLAG, RDB$INDICES.RDB$INDEX_TYPE 
		FROM RDB$INDICES 
		JOIN RDB$RELATIONS ON RDB$INDICES.RDB$RELATION_NAME = RDB$RELATIONS.RDB$RELATION_NAME 
		WHERE (RDB$RELATIONS.RDB$SYSTEM_FLAG <> 1 OR RDB$RELATIONS.RDB$SYSTEM_FLAG IS NULL);`

	rows, err := db.Query(query)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var index Index
		var unique sql.NullInt64
		if err = rows.Scan(
			&index.TableName,
			&index.Name,
			&unique,
			&index.Unique); err != nil {
			return
		}
		index.Unique.Bool, index.Unique.Valid = (unique.Int64 == 1), unique.Valid
		index.Name = strings.TrimRightFunc(index.Name, unicode.IsSpace)
		index.TableName = strings.TrimRightFunc(index.TableName, unicode.IsSpace)
		indexes = append(indexes, &index)
	}
	if err = rows.Err(); err != nil {
		return
	}
	for _, index := range indexes {
		if index.Columns, err = IndexColumnNames(db, index.Name); err != nil {
			return
		}
	}
	return
}

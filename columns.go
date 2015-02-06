package fbx

import (
	"database/sql"
	"fmt"
	"strings"
	"unicode"
)

type Column struct {
	Name         string
	Domain       string
	SqlType      string
	SqlSubtype   sql.NullInt64
	Length       int16 // DisplaySize
	Precision    sql.NullInt64
	Scale        int16
	Default      sql.NullString
	Nullable     sql.NullBool
	TypeCode     int
	InternalSize int
}

func Columns(db *sql.DB, tableName string) (columns []*Column, err error) {
	const query = `
		SELECT r.rdb$field_name, r.rdb$field_source, f.rdb$field_type, f.rdb$field_sub_type,
			f.rdb$field_length, f.rdb$field_precision, f.rdb$field_scale,
			COALESCE(r.rdb$default_source, f.rdb$default_source) rdb$default_source,
			COALESCE(r.rdb$null_flag, f.rdb$null_flag) rdb$null_flag
		FROM rdb$relation_fields r
		JOIN rdb$fields f ON r.rdb$field_source = f.rdb$field_name
		WHERE r.rdb$relation_name = ?
		ORDER BY r.rdb$field_position`

	rows, err := db.Query(query, tableName)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var col Column
		var sqlType int16
		if err = rows.Scan(
			&col.Name,
			&col.Domain,
			&sqlType,
			&col.SqlSubtype,
			&col.Length,
			&col.Precision,
			&col.Scale,
			&col.Default,
			&col.Nullable); err != nil {
			return
		}
		col.Name = strings.TrimRightFunc(col.Name, unicode.IsSpace)
		col.Domain = strings.TrimRightFunc(col.Domain, unicode.IsSpace)
		if strings.HasPrefix(col.Domain, "RDB$") {
			col.Domain = ""
		}
		col.SqlType = sqlTypeFromCode(int(sqlType), int(col.SqlSubtype.Int64))
		if col.Default.Valid {
			col.Default.String = strings.Replace(col.Default.String, "DEFAULT ", "", 1)
			col.Default.String = strings.TrimLeftFunc(col.Default.String, unicode.IsSpace)
		}
		columns = append(columns, &col)
	}
	err = rows.Err()
	return
}

func sqlTypeFromCode(code, subType int) string {
	switch code {
	case sql_text, blr_text:
		return "CHAR"
	case sql_varying, blr_varying:
		return "VARCHAR"
	case sql_short, blr_short:
		switch subType {
		case 0:
			return "SMALLINT"
		case 1:
			return "NUMERIC"
		case 2:
			return "DECIMAL"
		}
	case sql_long, blr_long:
		switch subType {
		case 0:
			return "INTEGER"
		case 1:
			return "NUMERIC"
		case 2:
			return "DECIMAL"
		}
		break
	case sql_float, blr_float:
		return "FLOAT"
	case sql_double, blr_double:
		switch subType {
		case 0:
			return "DOUBLE PRECISION"
		case 1:
			return "NUMERIC"
		case 2:
			return "DECIMAL"
		}
	case sql_d_float, blr_d_float:
		return "DOUBLE PRECISION"
	case sql_timestamp, blr_timestamp:
		return "TIMESTAMP"
	case sql_blob, blr_blob:
		return "BLOB"
	case sql_array:
		return "ARRAY"
	case sql_quad, blr_quad:
		return "DECIMAL"
	case sql_type_time, blr_sql_time:
		return "TIME"
	case sql_type_date, blr_sql_date:
		return "DATE"
	case sql_int64, blr_int64:
		switch subType {
		case 0:
			return "BIGINT"
		case 1:
			return "NUMERIC"
		case 2:
			return "DECIMAL"
		}
	}
	return fmt.Sprintf("UNKNOWN %d, %d", code, subType)
}

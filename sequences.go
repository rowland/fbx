package fbx

import (
	"database/sql"
	"fmt"
)

func NextSequenceValue(db *sql.DB, name string) (value int64, err error) {
	query := fmt.Sprintf("SELECT NEXT VALUE FOR %s FROM RDB$DATABASE", name)
	err = db.QueryRow(query).Scan(&value)
	return
}

package fbx

import (
	"database/sql"
	_ "github.com/rowland/firebirdsql"
	"testing"
)

func TestNextSequenceValue(t *testing.T) {
	const sqlSchema = "CREATE GENERATOR TEST;"

	db, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/fbx_test_namenames.fdb")
	if err != nil {
		t.Fatalf("Error creating database: %s", err)
	}
	defer db.Close()

	err = ExecScript(db, sqlSchema)
	if err != nil {
		t.Fatal(err)
	}

	for id := 1; id <= 10; id++ {
		v, err := NextSequenceValue(db, "TEST")
		if err != nil {
			t.Fatal(err)
		}
		if v != int64(id) {
			t.Errorf("Expected <%d>, got <%d>.", id, v)
		}
	}
}

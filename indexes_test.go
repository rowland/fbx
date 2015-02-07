package fbx

import (
	"database/sql"
	_ "github.com/rowland/firebirdsql"
	"testing"
)

func TestIndexes(t *testing.T) {
	const sqlSchema = `
		CREATE TABLE TEST(ID INT NOT NULL, NAME VARCHAR(20) NOT NULL);
		ALTER TABLE TEST ADD CONSTRAINT PK PRIMARY KEY(ID, NAME);`

	db, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/fbx_test_indexes.fdb")
	if err != nil {
		t.Fatalf("Error creating database: %s", err)
	}
	defer db.Close()

	err = ExecScript(db, sqlSchema)
	if err != nil {
		t.Fatal(err)
	}

	indexes, err := Indexes(db)
	if err != nil {
		t.Fatal(err)
	}

	if len(indexes) != 1 {
		t.Fatalf("Expected 1 index, got %d", len(indexes))
	}
	if indexes[0].Name != "PK" {
		t.Errorf("Expected Name <%s>, got <%s>", "PK", indexes[0].Name)
	}
	if indexes[0].TableName != "TEST" {
		t.Errorf("Expected TableName <%s>, got <%s>", "TEST", indexes[0].TableName)
	}
	if !indexes[0].Unique.Bool {
		t.Error("Expected Unique to be true")
	}
	if indexes[0].Descending.Bool {
		t.Error("Expected Descending to be false")
	}
	if len(indexes[0].Columns) != 2 {
		t.Fatalf("Expected <%d> Columns, got <%d>", 2, len(indexes[0].Columns))
	}
	if indexes[0].Columns[0] != "ID" {
		t.Errorf("Expected Column <%s>, got <%s>", "ID", indexes[0].Columns[0])
	}
	if indexes[0].Columns[1] != "NAME" {
		t.Errorf("Expected Column <%s>, got <%s>", "NAME", indexes[0].Columns[1])
	}
}

func TestIndexesOnTable(t *testing.T) {
	const sqlSchema = `
		CREATE TABLE TEST1(ID INT NOT NULL, NAME VARCHAR(20) NOT NULL);
		ALTER TABLE TEST1 ADD CONSTRAINT PK1 PRIMARY KEY(ID, NAME);
		CREATE TABLE TEST2(ID INT NOT NULL, NAME VARCHAR(20) NOT NULL);
		ALTER TABLE TEST2 ADD CONSTRAINT PK2 PRIMARY KEY(ID, NAME);`

	db, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/fbx_test_indexes_on_table.fdb")
	if err != nil {
		t.Fatalf("Error creating database: %s", err)
	}
	defer db.Close()

	err = ExecScript(db, sqlSchema)
	if err != nil {
		t.Fatal(err)
	}

	indexes, err := IndexesOnTable(db, "TEST2")
	if err != nil {
		t.Fatal(err)
	}

	if len(indexes) != 1 {
		t.Fatalf("Expected 1 index, got %d", len(indexes))
	}
	if indexes[0].Name != "PK2" {
		t.Errorf("Expected Name <%s>, got <%s>", "PK2", indexes[0].Name)
	}
	if indexes[0].TableName != "TEST2" {
		t.Errorf("Expected TableName <%s>, got <%s>", "TEST2", indexes[0].TableName)
	}
	if !indexes[0].Unique.Bool {
		t.Error("Expected Unique to be true")
	}
	if indexes[0].Descending.Bool {
		t.Error("Expected Descending to be false")
	}
	if len(indexes[0].Columns) != 2 {
		t.Fatalf("Expected <%d> Columns, got <%d>", 2, len(indexes[0].Columns))
	}
	if indexes[0].Columns[0] != "ID" {
		t.Errorf("Expected Column <%s>, got <%s>", "ID", indexes[0].Columns[0])
	}
	if indexes[0].Columns[1] != "NAME" {
		t.Errorf("Expected Column <%s>, got <%s>", "NAME", indexes[0].Columns[1])
	}
}

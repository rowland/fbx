package fbx

import (
	"database/sql"
	_ "github.com/rowland/firebirdsql"
	"reflect"
	"testing"
)

var expectedColumnNames = []string{"ID", "FLAG", "BINARY", "I", "I32", "I64", "F32", "F64", "C", "CS", "V", "VS", "M", "DT", "TM", "TS", "N92", "D92"}

func TestColumnNames(t *testing.T) {
	db, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/fbx_test_column_names.fdb")
	if err != nil {
		t.Fatalf("Error creating database: %s", err)
	}
	defer db.Close()

	err = ExecScript(db, sqlSampleSchema)
	if err != nil {
		t.Fatal(err)
	}

	columnNames, err := ColumnNames(db, "TEST")
	if err != nil {
		t.Fatal(err)
	}

	if len(columnNames) != len(expectedColumnNames) {
		t.Fatalf("Expected %d column names, got %d", len(expectedColumnNames), len(columnNames))
	}
	for i := range columnNames {
		if columnNames[i] != expectedColumnNames[i] {
			t.Errorf("Expected column name <%s>, got <%s>", expectedColumnNames[i], columnNames[i])
		}
	}
}

func TestIndexColumnNames(t *testing.T) {
	const sqlSchema = `
		CREATE TABLE TEST(ID INT NOT NULL, NAME VARCHAR(20) NOT NULL);
		ALTER TABLE TEST ADD CONSTRAINT PK PRIMARY KEY(ID, NAME);`

	db, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/fbx_test_index_column_names.fdb")
	if err != nil {
		t.Fatalf("Error creating database: %s", err)
	}
	defer db.Close()

	err = ExecScript(db, sqlSchema)
	if err != nil {
		t.Fatal(err)
	}

	var pk []string
	if pk, err = IndexColumnNames(db, "PK"); err != nil {
		t.Fatal(err)
	}

	exp := []string{"ID", "NAME"}
	if !reflect.DeepEqual(exp, pk) {
		t.Errorf("Expected %v, got %v", exp, pk)
	}
}

func TestSequenceNames(t *testing.T) {
	const sqlSchema = `
		CREATE SEQUENCE TEST1_SEQ;
		CREATE SEQUENCE TEST2_SEQ;
	`

	db, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/fbx_test_sequence_names.fdb")
	if err != nil {
		t.Fatalf("Error creating database: %s", err)
	}
	defer db.Close()

	err = ExecScript(db, sqlSchema)
	if err != nil {
		t.Fatal(err)
	}

	sequenceNames, err := SequenceNames(db)
	if err != nil {
		t.Fatal(err)
	}

	if len(sequenceNames) != 2 {
		t.Fatal("Expected 2 sequence names.")
	}
	if sequenceNames[0] != "TEST1_SEQ" {
		t.Errorf("Expected <TEST1_SEQ>, got <%s>.", sequenceNames[0])
	}
	if sequenceNames[1] != "TEST2_SEQ" {
		t.Errorf("Expected <TEST2_SEQ>, got <%s>.", sequenceNames[1])
	}
}

func TestTableNames(t *testing.T) {
	const sqlSchema = "CREATE TABLE TEST1 (ID INTEGER); CREATE TABLE TEST2 (ID INTEGER);"

	db, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/fbx_test_table_names.fdb")
	if err != nil {
		t.Fatalf("Error creating database: %s", err)
	}
	defer db.Close()

	err = ExecScript(db, sqlSchema)
	if err != nil {
		t.Fatal(err)
	}

	tableNames, err := TableNames(db)
	if err != nil {
		t.Fatal(err)
	}

	if len(tableNames) != 2 {
		t.Fatal("Expected 2 table names.")
	}
	if tableNames[0] != "TEST1" {
		t.Errorf("Expected <TEST1>, got <%s>.", tableNames[0])
	}
	if tableNames[1] != "TEST2" {
		t.Errorf("Expected <TEST2>, got <%s>.", tableNames[1])
	}
}

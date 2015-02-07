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

func TestProcedureNames(t *testing.T) {
	const sqlSchema = `
		CREATE PROCEDURE PLUSONE(NUM1 INTEGER) RETURNS (NUM2 INTEGER) AS
		BEGIN
		  NUM2 = NUM1 + 1;
		  SUSPEND;
		END;`

	db, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/fbx_test_procedure_names.fdb")
	if err != nil {
		t.Fatalf("Error creating database: %s", err)
	}
	defer db.Close()

	_, err = db.Exec(sqlSchema)
	if err != nil {
		t.Fatal(err)
	}

	procNames, err := ProcedureNames(db)
	if err != nil {
		t.Fatal(err)
	}
	if len(procNames) != 1 {
		t.Fatal("Expected %d role names, got %d", 1, len(procNames))
	}
	if procNames[0] != "PLUSONE" {
		t.Errorf("Expected <%s>, got <%s>", "PLUSONE", procNames[0])
	}
}

func TestRoleNames(t *testing.T) {
	const sqlSchema = `
		CREATE ROLE READER;
		CREATE ROLE WRITER;`

	db, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/fbx_test_role_names.fdb")
	if err != nil {
		t.Fatalf("Error creating database: %s", err)
	}
	defer db.Close()

	err = ExecScript(db, sqlSchema)
	if err != nil {
		t.Fatal(err)
	}

	roleNames, err := RoleNames(db)
	if err != nil {
		t.Fatal(err)
	}

	if len(roleNames) != 2 {
		t.Fatal("Expected 2 role names.")
	}
	if roleNames[0] != "READER" {
		t.Errorf("Expected <READER>, got <%s>.", roleNames[0])
	}
	if roleNames[1] != "WRITER" {
		t.Errorf("Expected <WRITER>, got <%s>.", roleNames[1])
	}
}

func TestSequenceNames(t *testing.T) {
	const sqlSchema = `
		CREATE SEQUENCE TEST1_SEQ;
		CREATE SEQUENCE TEST2_SEQ;`

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

func TestTriggerNames(t *testing.T) {
	const sqlSchema = `
		CREATE TABLE TEST (ID INT, NAME VARCHAR(20));
		CREATE GENERATOR TEST_SEQ;`
	const triggerSchema = `
		CREATE TRIGGER TEST_INSERT FOR TEST ACTIVE BEFORE INSERT AS
		BEGIN
			IF (NEW.ID IS NULL) THEN
				NEW.ID = CAST(GEN_ID(TEST_SEQ, 1) AS INT);
		END`

	db, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/fbx_test_trigger_names.fdb")
	if err != nil {
		t.Fatalf("Error creating database: %s", err)
	}
	defer db.Close()

	err = ExecScript(db, sqlSchema)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(triggerSchema)
	if err != nil {
		t.Fatal(err)
	}

	triggerNames, err := TriggerNames(db)
	if err != nil {
		t.Fatal(err)
	}

	if len(triggerNames) != 1 {
		t.Fatal("Expected %d trigger names, got %d", 1, len(triggerNames))
	}
	if triggerNames[0] != "TEST_INSERT" {
		t.Errorf("Expected <%s>, got <%s>", triggerNames[0])
	}
}

func TestViewNames(t *testing.T) {
	const sqlSchema = `
		CREATE TABLE TEST1 (ID INT, NAME1 VARCHAR(10));
		CREATE TABLE TEST2 (ID INT, NAME2 VARCHAR(10));
		CREATE VIEW VIEW1 AS SELECT TEST1.ID, TEST1.NAME1, TEST2.NAME2 FROM TEST1 JOIN TEST2 ON TEST1.ID = TEST2.ID;
		CREATE VIEW VIEW2 AS SELECT TEST2.ID, TEST1.NAME1, TEST2.NAME2 FROM TEST1 JOIN TEST2 ON TEST1.NAME1 = TEST2.NAME2;`

	db, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/fbx_test_view_names.fdb")
	if err != nil {
		t.Fatalf("Error creating database: %s", err)
	}
	defer db.Close()

	err = ExecScript(db, sqlSchema)
	if err != nil {
		t.Fatal(err)
	}

	viewNames, err := ViewNames(db)
	if err != nil {
		t.Fatal(err)
	}

	if len(viewNames) != 2 {
		t.Fatal("Expected 2 table names.")
	}
	if viewNames[0] != "VIEW1" {
		t.Errorf("Expected <VIEW1>, got <%s>.", viewNames[0])
	}
	if viewNames[1] != "VIEW2" {
		t.Errorf("Expected <VIEW2>, got <%s>.", viewNames[1])
	}
}

package fbx

import (
	"database/sql"
	_ "github.com/rowland/firebirdsql"
	"reflect"
	"testing"
)

// Length of CHAR and VARCHAR fields assumes UTF8 character set.
var expectedColumns = []Column{
	{Name: "ID", Domain: "", SqlType: "BIGINT", SqlSubtype: sql.NullInt64{0, true}, Length: 8, Precision: sql.NullInt64{0, true}, Scale: 0, Default: sql.NullString{"", false}, Nullable: sql.NullBool{true, true}},
	{Name: "FLAG", Domain: "BOOLEAN", SqlType: "INTEGER", SqlSubtype: sql.NullInt64{0, true}, Length: 4, Precision: sql.NullInt64{0, true}, Scale: 0, Default: sql.NullString{"", false}, Nullable: sql.NullBool{false, false}},
	{Name: "BINARY", Domain: "", SqlType: "BLOB", SqlSubtype: sql.NullInt64{0, true}, Length: 8, Precision: sql.NullInt64{0, false}, Scale: 0, Default: sql.NullString{"", false}, Nullable: sql.NullBool{false, false}},
	{Name: "I", Domain: "", SqlType: "INTEGER", SqlSubtype: sql.NullInt64{0, true}, Length: 4, Precision: sql.NullInt64{0, true}, Scale: 0, Default: sql.NullString{"", false}, Nullable: sql.NullBool{false, false}},
	{Name: "I32", Domain: "", SqlType: "INTEGER", SqlSubtype: sql.NullInt64{0, true}, Length: 4, Precision: sql.NullInt64{0, true}, Scale: 0, Default: sql.NullString{"0", true}, Nullable: sql.NullBool{false, false}},
	{Name: "I64", Domain: "", SqlType: "BIGINT", SqlSubtype: sql.NullInt64{0, true}, Length: 8, Precision: sql.NullInt64{0, true}, Scale: 0, Default: sql.NullString{"", false}, Nullable: sql.NullBool{false, false}},
	{Name: "F32", Domain: "", SqlType: "FLOAT", SqlSubtype: sql.NullInt64{0, false}, Length: 4, Precision: sql.NullInt64{0, false}, Scale: 0, Default: sql.NullString{"", false}, Nullable: sql.NullBool{false, false}},
	{Name: "F64", Domain: "", SqlType: "DOUBLE PRECISION", SqlSubtype: sql.NullInt64{0, false}, Length: 8, Precision: sql.NullInt64{0, false}, Scale: 0, Default: sql.NullString{"0.0", true}, Nullable: sql.NullBool{false, false}},
	{Name: "C", Domain: "", SqlType: "CHAR", SqlSubtype: sql.NullInt64{0, true}, Length: 4, Precision: sql.NullInt64{0, false}, Scale: 0, Default: sql.NullString{"", false}, Nullable: sql.NullBool{false, false}},
	{Name: "CS", Domain: "ALPHABET", SqlType: "CHAR", SqlSubtype: sql.NullInt64{0, true}, Length: 104, Precision: sql.NullInt64{0, false}, Scale: 0, Default: sql.NullString{"", false}, Nullable: sql.NullBool{false, false}},
	{Name: "V", Domain: "", SqlType: "VARCHAR", SqlSubtype: sql.NullInt64{0, true}, Length: 4, Precision: sql.NullInt64{0, false}, Scale: 0, Default: sql.NullString{"", false}, Nullable: sql.NullBool{false, false}},
	{Name: "VS", Domain: "ALPHA", SqlType: "VARCHAR", SqlSubtype: sql.NullInt64{0, true}, Length: 104, Precision: sql.NullInt64{0, false}, Scale: 0, Default: sql.NullString{"", false}, Nullable: sql.NullBool{false, false}},
	{Name: "M", Domain: "", SqlType: "BLOB", SqlSubtype: sql.NullInt64{1, true}, Length: 8, Precision: sql.NullInt64{0, false}, Scale: 0, Default: sql.NullString{"", false}, Nullable: sql.NullBool{false, false}},
	{Name: "DT", Domain: "", SqlType: "DATE", SqlSubtype: sql.NullInt64{0, false}, Length: 4, Precision: sql.NullInt64{0, false}, Scale: 0, Default: sql.NullString{"", false}, Nullable: sql.NullBool{false, false}},
	{Name: "TM", Domain: "", SqlType: "TIME", SqlSubtype: sql.NullInt64{0, false}, Length: 4, Precision: sql.NullInt64{0, false}, Scale: 0, Default: sql.NullString{"", false}, Nullable: sql.NullBool{false, false}},
	{Name: "TS", Domain: "", SqlType: "TIMESTAMP", SqlSubtype: sql.NullInt64{0, false}, Length: 8, Precision: sql.NullInt64{0, false}, Scale: 0, Default: sql.NullString{"", false}, Nullable: sql.NullBool{false, false}},
	{Name: "N92", Domain: "", SqlType: "NUMERIC", SqlSubtype: sql.NullInt64{1, true}, Length: 4, Precision: sql.NullInt64{9, true}, Scale: -2, Default: sql.NullString{"", false}, Nullable: sql.NullBool{false, false}},
	{Name: "D92", Domain: "", SqlType: "DECIMAL", SqlSubtype: sql.NullInt64{2, true}, Length: 4, Precision: sql.NullInt64{9, true}, Scale: -2, Default: sql.NullString{"", false}, Nullable: sql.NullBool{false, false}},
}

const sqlSampleSchema = `
	CREATE DOMAIN ALPHA VARCHAR(26);
	CREATE DOMAIN ALPHABET CHAR(26);
	CREATE DOMAIN BOOLEAN INTEGER CHECK ((VALUE IN (0,1)) OR (VALUE IS NULL));
	CREATE TABLE TEST (
		ID BIGINT PRIMARY KEY NOT NULL,
		FLAG BOOLEAN,
		BINARY BLOB,
		I INTEGER,
		I32 INTEGER DEFAULT 0,
		I64 BIGINT,
		F32 FLOAT,
		F64 DOUBLE PRECISION DEFAULT    0.0,
		C CHAR,
		CS ALPHABET,
		V VARCHAR(1),
		VS ALPHA,
		M BLOB SUB_TYPE TEXT,
		DT DATE,
		TM TIME,
		TS TIMESTAMP,
		N92 NUMERIC(9,2),
		D92 DECIMAL(9,2));`

func TestColumns(t *testing.T) {
	db, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/fbx_test_columns.fdb")
	if err != nil {
		t.Fatalf("Error creating database: %s", err)
	}
	defer db.Close()

	err = ExecScript(db, sqlSampleSchema)
	if err != nil {
		t.Fatal(err)
	}

	cols, err := Columns(db, "TEST")
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 18 {
		t.Fatalf("Expected <18>, got <%d>.", len(cols))
	}
	for i, exp := range expectedColumns {
		if !reflect.DeepEqual(&exp, cols[i]) {
			t.Errorf("Expected %#v,\n got %#v", &exp, cols[i])
		}
	}
}

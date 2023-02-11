package bstore

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestExport(t *testing.T) {
	type User1 struct {
		ID   int
		Name string
	}

	type User2 struct {
		ID      int
		String  string
		Time    time.Time
		Bool    bool
		Boolptr *bool
		Uint    uint64
		Bytes   []byte
		Struct  struct {
			Value int
		}
		Slice  []string
		Slice2 []struct {
			Name string
		}
		Map     map[string]int
		Map2    map[string]struct{ Name string }
		Float32 float32
		Float64 float64
		BM      bm
	}

	os.Remove("testdata/export.db")
	db, err := topen(t, "testdata/export.db", nil, User1{}, User2{})
	tcheck(t, err, "open")
	defer db.Close()

	var ids2 []int
	err = db.Write(func(tx *Tx) error {
		u0 := User1{0, "hi"}
		err = tx.Insert(&u0)
		tcheck(t, err, "insert")

		u1 := User1{0, "hi"}
		err = tx.Insert(&u1)
		tcheck(t, err, "insert")

		u2 := User2{}
		err = tx.Insert(&u2)
		tcheck(t, err, "insert zero")

		xfalse := false
		u3 := User2{
			0,
			"test",
			time.Now(),
			true,
			&xfalse,
			123,
			[]byte("hi"),
			struct{ Value int }{1},
			[]string{"a", "b"},
			[]struct{ Name string }{{"x"}, {""}},
			map[string]int{"a": 123, "b": 0},
			map[string]struct{ Name string }{"x": {"y"}, "y": {""}},
			1.23,
			-100.34,
			bm{"test"},
		}
		err = tx.Insert(&u3)
		tcheck(t, err, "insert nonzero")

		u4 := User2{
			0,
			"test",
			time.Now(),
			true,
			nil,
			123,
			[]byte{},
			struct{ Value int }{0},
			[]string{},
			[]struct{ Name string }{},
			map[string]int{},
			map[string]struct{ Name string }{},
			33.44,
			101,
			bm{""},
		}
		err = tx.Insert(&u4)
		tcheck(t, err, "insert different nonzero")

		ids2 = []int{u2.ID, u3.ID, u4.ID}

		return nil
	})
	tcheck(t, err, "write")

	var xids2 []int
	err = db.Keys("User2", func(id any) error {
		xids2 = append(xids2, id.(int))
		return nil
	})
	tcompare(t, err, xids2, ids2, "keys")

	var fields []string
	expFields := []string{"ID", "String", "Time", "Bool", "Boolptr", "Uint", "Bytes", "Struct", "Slice", "Slice2", "Map", "Map2", "Float32", "Float64", "BM"}
	xids2 = nil
	err = db.Records("User2", &fields, func(v map[string]any) error {
		xids2 = append(xids2, v["ID"].(int))
		return nil
	})
	tcompare(t, err, xids2, ids2, "record ids")
	tcompare(t, err, expFields, fields, "records fields")

	fields = nil
	record, err := db.Record("User2", fmt.Sprintf("%d", ids2[0]), &fields)
	tcompare(t, err, expFields, fields, "record fields")
	tcompare(t, err, record["ID"].(int), ids2[0], "record id")
}

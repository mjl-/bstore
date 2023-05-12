package bstore

import (
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
)

func tneedpkkey[T any](t *testing.T, openErr, insertErr error, v T, field string) {
	t.Helper()

	os.Remove("testdata/pkkeys.db")
	db, err := topen(t, "testdata/pkkeys.db", nil, v)
	if openErr != nil {
		tneed(t, err, openErr, "open/register")
		return
	}
	tcheck(t, err, "open")
	defer tclose(t, db)

	err = db.Insert(ctxbg, &v)
	if insertErr != nil {
		tneed(t, err, insertErr, "insert")
		return
	}
	tcheck(t, err, "insert")

	err = db.Get(ctxbg, &v)
	tcheck(t, err, "get")

	l, err := QueryDB[T](ctxbg, db).List()
	tcompare(t, err, l, []T{v}, "list")

	// Fetch by index.
	fv := reflect.ValueOf(v).FieldByName(field).Interface()
	l, err = QueryDB[T](ctxbg, db).FilterEqual(field, fv).List()
	tcompare(t, err, l, []T{v}, "list by equal")

	pkv := reflect.ValueOf(v).Field(0).Interface()
	if b, ok := pkv.([]byte); ok {
		pkv = string(b) // Bytes keys as strings.
	}
	tname, err := typeName(reflect.TypeOf(v))
	tcheck(t, err, "typename")
	var fields []string
	err = db.Read(ctxbg, func(tx *Tx) error {
		_, err = tx.Record(tname, fmt.Sprintf("%v", pkv), &fields)
		tcheck(t, err, "record as map")
		return nil
	})
	tcheck(t, err, "tx record")
}

func TestKeys(t *testing.T) {
	type Auto[T any] struct {
		PK T `bstore:"typename Auto"`
	}

	type Noauto[T any] struct {
		PK T `bstore:"noauto,typename Noauto"`
	}

	type Index[T any] struct {
		PK    int `bstore:"typename Index"`
		Field T   `bstore:"index"`
	}

	type Unique[T any] struct {
		PK    int `bstore:"typename Unique"`
		Field T   `bstore:"unique"`
	}

	type Struct struct {
		Field int
	}

	type Map map[string]int

	// Autoincrement with integer PK's.
	tneedpkkey(t, nil, nil, Auto[uint8]{0}, "PK")
	tneedpkkey(t, nil, nil, Auto[uint16]{0}, "PK")
	tneedpkkey(t, nil, nil, Auto[uint32]{0}, "PK")
	tneedpkkey(t, nil, nil, Auto[uint64]{0}, "PK")
	tneedpkkey(t, nil, nil, Auto[uint]{0}, "PK")

	tneedpkkey(t, nil, nil, Auto[int8]{0}, "PK")
	tneedpkkey(t, nil, nil, Auto[int16]{0}, "PK")
	tneedpkkey(t, nil, nil, Auto[int32]{0}, "PK")
	tneedpkkey(t, nil, nil, Auto[int64]{0}, "PK")
	tneedpkkey(t, nil, nil, Auto[int]{0}, "PK")

	// Cannot insert zero values for non-auto fields.
	tneedpkkey(t, nil, ErrZero, Auto[string]{""}, "")
	tneedpkkey(t, nil, ErrZero, Auto[bool]{false}, "")
	tneedpkkey(t, nil, ErrZero, Auto[[]byte]{[]byte(nil)}, "")

	// Cannot use other types as index.
	tneedpkkey(t, ErrType, nil, Auto[time.Time]{time.Now()}, "")
	tneedpkkey(t, ErrType, nil, Auto[[]string]{nil}, "")
	tneedpkkey(t, ErrType, nil, Auto[Struct]{Struct{1}}, "")
	tneedpkkey(t, ErrType, nil, Auto[Map]{Map{"a": 1}}, "")

	// Inserting non-zero PK's is fine.
	tneedpkkey(t, nil, nil, Auto[string]{"test"}, "PK")
	tneedpkkey(t, nil, nil, Auto[bool]{true}, "PK")
	tneedpkkey(t, nil, nil, Auto[[]byte]{[]byte("test")}, "PK")

	// Cannot insert zero values for explicit nonauto integer PK's.
	tneedpkkey(t, nil, ErrParam, Noauto[uint8]{0}, "")
	tneedpkkey(t, nil, ErrParam, Noauto[uint16]{0}, "")
	tneedpkkey(t, nil, ErrParam, Noauto[uint32]{0}, "")
	tneedpkkey(t, nil, ErrParam, Noauto[uint64]{0}, "")
	tneedpkkey(t, nil, ErrParam, Noauto[uint]{0}, "")

	tneedpkkey(t, nil, ErrParam, Noauto[int8]{0}, "")
	tneedpkkey(t, nil, ErrParam, Noauto[int16]{0}, "")
	tneedpkkey(t, nil, ErrParam, Noauto[int32]{0}, "")
	tneedpkkey(t, nil, ErrParam, Noauto[int64]{0}, "")
	tneedpkkey(t, nil, ErrParam, Noauto[int]{0}, "")

	// Inserting non-zero PK's is fine.
	tneedpkkey(t, nil, nil, Noauto[uint8]{1}, "PK")
	tneedpkkey(t, nil, nil, Noauto[uint16]{1}, "PK")
	tneedpkkey(t, nil, nil, Noauto[uint32]{1}, "PK")
	tneedpkkey(t, nil, nil, Noauto[uint64]{1}, "PK")
	tneedpkkey(t, nil, nil, Noauto[uint]{1}, "PK")

	tneedpkkey(t, nil, nil, Noauto[int8]{1}, "PK")
	tneedpkkey(t, nil, nil, Noauto[int16]{1}, "PK")
	tneedpkkey(t, nil, nil, Noauto[int32]{1}, "PK")
	tneedpkkey(t, nil, nil, Noauto[int64]{1}, "PK")
	tneedpkkey(t, nil, nil, Noauto[int]{1}, "PK")

	// Cannot have "noauto" on non-integer field.
	tneedpkkey(t, ErrType, nil, Noauto[string]{"a"}, "")
	tneedpkkey(t, ErrType, nil, Noauto[bool]{true}, "")
	tneedpkkey(t, ErrType, nil, Noauto[[]byte]{[]byte("a")}, "")
	tneedpkkey(t, ErrType, nil, Auto[Map]{Map{"a": 1}}, "")

	// Inserting non-zero PK's is fine.
	tneedpkkey(t, nil, nil, Index[uint8]{0, 1}, "Field")
	tneedpkkey(t, nil, nil, Index[uint16]{0, 1}, "Field")
	tneedpkkey(t, nil, nil, Index[uint32]{0, 1}, "Field")
	tneedpkkey(t, nil, nil, Index[uint64]{0, 1}, "Field")
	tneedpkkey(t, nil, nil, Index[uint]{0, 1}, "Field")

	tneedpkkey(t, nil, nil, Index[int8]{0, 1}, "Field")
	tneedpkkey(t, nil, nil, Index[int16]{0, 1}, "Field")
	tneedpkkey(t, nil, nil, Index[int32]{0, 1}, "Field")
	tneedpkkey(t, nil, nil, Index[int64]{0, 1}, "Field")
	tneedpkkey(t, nil, nil, Index[int]{0, 1}, "Field")

	tneedpkkey(t, nil, nil, Index[string]{0, "a"}, "Field")
	tneedpkkey(t, nil, nil, Index[bool]{0, true}, "Field")
	tneedpkkey(t, nil, nil, Index[time.Time]{0, time.Now()}, "Field")
	tneedpkkey(t, nil, nil, Index[[]string]{0, []string{"a", "b"}}, "Field")

	// Cannot use other types as index.
	tneedpkkey(t, ErrType, nil, Index[[]byte]{0, []byte("a")}, "")
	tneedpkkey(t, ErrType, nil, Index[Struct]{0, Struct{1}}, "")
	tneedpkkey(t, ErrType, nil, Index[Map]{0, Map{"a": 1}}, "")

	// Inserting non-zero PK's is fine.
	tneedpkkey(t, nil, nil, Unique[uint8]{0, 1}, "Field")
	tneedpkkey(t, nil, nil, Unique[uint16]{0, 1}, "Field")
	tneedpkkey(t, nil, nil, Unique[uint32]{0, 1}, "Field")
	tneedpkkey(t, nil, nil, Unique[uint64]{0, 1}, "Field")
	tneedpkkey(t, nil, nil, Unique[uint]{0, 1}, "Field")

	tneedpkkey(t, nil, nil, Unique[int8]{0, 1}, "Field")
	tneedpkkey(t, nil, nil, Unique[int16]{0, 1}, "Field")
	tneedpkkey(t, nil, nil, Unique[int32]{0, 1}, "Field")
	tneedpkkey(t, nil, nil, Unique[int64]{0, 1}, "Field")
	tneedpkkey(t, nil, nil, Unique[int]{0, 1}, "Field")

	tneedpkkey(t, nil, nil, Unique[string]{0, "a"}, "Field")
	tneedpkkey(t, nil, nil, Unique[bool]{0, true}, "Field")
	tneedpkkey(t, nil, nil, Unique[time.Time]{0, time.Now()}, "Field")

	// Cannot use other types as index.
	tneedpkkey(t, ErrType, nil, Unique[[]byte]{0, []byte("a")}, "")
	tneedpkkey(t, ErrType, nil, Unique[[]string]{0, nil}, "")
	tneedpkkey(t, ErrType, nil, Unique[Struct]{0, Struct{1}}, "")
	tneedpkkey(t, ErrType, nil, Unique[Map]{0, Map{"a": 1}}, "")

	// Multiple keys with slice field are not allowed.
	type MultipleSliceFields struct {
		ID int64
		A  []string
		B  []int `bstore:"index B+A"`
	}
	tneedpkkey(t, ErrType, nil, MultipleSliceFields{}, "")
}

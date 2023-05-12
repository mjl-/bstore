package bstore

import (
	"os"
	"testing"
	"time"
)

func TestDefault(t *testing.T) {
	type User struct {
		ID int

		Bool    bool      `bstore:"default false"`
		BoolT   bool      `bstore:"default true"`
		Int     int       `bstore:"default 100"`
		Int8    int8      `bstore:"default 8"`
		Int16   int16     `bstore:"default 200"`
		Int32   int32     `bstore:"default 300"`
		Int64   int64     `bstore:"default 400"`
		Uint    uint      `bstore:"default 32"`
		Uint8   uint8     `bstore:"default 8"`
		Uint16  uint16    `bstore:"default 16"`
		Uint32  uint32    `bstore:"default 32"`
		Uint64  uint64    `bstore:"default 64"`
		Float32 float32   `bstore:"default 10.5"`
		Float64 float64   `bstore:"default 111.11"`
		String  string    `bstore:"default string"`
		Time    time.Time `bstore:"default now"`
		Time2   time.Time `bstore:"default 2022-10-18T07:05:02-02:00"`

		Intptr    *int       `bstore:"default 100"`
		Stringptr *string    `bstore:"default string"`
		Timeptr   *time.Time `bstore:"default now"`
	}

	type Struct struct {
		Value int `bstore:"default 123"`
	}

	type Deep struct {
		ID     int
		Struct Struct
		Slice  []Struct
	}

	type Bad1 struct {
		ID int `bstore:"default 1"` // Not allowed on primary keys.
	}

	type Bad2 struct {
		ID  int
		Int int `bstore:"default invalid"` // Invalid int.
	}

	type Bad3 struct {
		ID  int
		Int int `bstore:"default"` // Missing parameter.
	}

	type Bad4 struct {
		ID   int
		Time time.Time `bstore:"default bogus"` // Invalid time.
	}

	type Bad5 struct {
		ID  int
		Map map[string]Struct // Invalid default inside map.
	}

	os.Remove("testdata/default.db")

	_, err := topen(t, "testdata/default.db", nil, Bad1{})
	tneed(t, err, ErrType, "bad1")

	_, err = topen(t, "testdata/default.db", nil, Bad2{})
	tneed(t, err, ErrType, "bad2")

	_, err = topen(t, "testdata/default.db", nil, Bad3{})
	tneed(t, err, ErrType, "bad3")

	_, err = topen(t, "testdata/default.db", nil, Bad4{})
	tneed(t, err, ErrType, "bad4")

	_, err = topen(t, "testdata/default.db", nil, Bad5{})
	tneed(t, err, ErrType, "bad5")

	db, err := topen(t, "testdata/default.db", nil, User{}, Deep{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	now := time.Now().Round(0)
	u0 := User{0, true, true, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0.5, 0.5, "x", now, now, nil, nil, nil}
	u0.Intptr = &u0.Int
	u0.Stringptr = &u0.String
	u0.Timeptr = &u0.Time
	x0 := u0

	err = db.Insert(ctxbg, &u0)
	x0.ID = u0.ID
	tcompare(t, err, u0, x0, "insert with nonzero values")

	tmdef, err := time.Parse(time.RFC3339, "2022-10-18T07:05:02-02:00")
	tcheck(t, err, "parse time")
	var u1 User
	err = db.Insert(ctxbg, &u1)
	x1 := User{u1.ID, false, true, 100, 8, 200, 300, 400, 32, 8, 16, 32, 64, 10.5, 111.11, "string", u1.Time, tmdef, &u1.Int, &u1.String, u1.Timeptr}
	tcompare(t, err, u1, x1, "insert with zero and default values")
	if u1.Time.Sub(now) > time.Second {
		t.Fatalf("time was to %s, not current time %s", u1.Time, now)
	}
	if u1.Timeptr.Sub(now) > time.Second {
		t.Fatalf("timeptr was to %s, not current time %s", u1.Timeptr, now)
	}

	d0 := Deep{0, Struct{0}, []Struct{{0}}}
	err = db.Insert(ctxbg, &d0)
	xd0 := Deep{d0.ID, Struct{123}, []Struct{{123}}}
	tcompare(t, err, d0, xd0, "deeper default values")
}

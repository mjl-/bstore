//go:build !(386 || arm || mips || mipsle)

package bstore

import (
	"math"
	"os"
	"testing"
)

func TestIntwidth(t *testing.T) {
	// int is treated as int32, for portability between 32 and 64 bit (int)
	// systems. We detect values that are too large.  We have types for int
	// and uint as PK, since code for packing keys is different from the
	// other fields.

	type User struct {
		ID   int
		Uint uint
	}

	type User2 struct {
		ID  uint
		Int int
	}
	const path = "testdata/tmp.intwidth.db"
	os.Remove(path)
	db, err := topen(t, path, nil, User{}, User2{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	err = db.Insert(ctxbg, &User{math.MinInt32 - 1, 1})
	tneed(t, err, ErrParam, "out of range")

	err = db.Insert(ctxbg, &User{math.MaxInt32 + 1, 1})
	tneed(t, err, ErrParam, "out of range")

	err = db.Insert(ctxbg, &User{0, math.MaxUint32 + 1})
	tneed(t, err, ErrParam, "out of range")

	u0 := User{math.MinInt32, 1}
	err = db.Insert(ctxbg, &u0)
	tcheck(t, err, "insert user")
	err = db.Get(ctxbg, &u0)
	tcompare(t, err, u0, User{math.MinInt32, 1}, "get user")

	u1 := User{math.MaxInt32, 1}
	err = db.Insert(ctxbg, &u1)
	tcheck(t, err, "insert user")
	err = db.Get(ctxbg, &u1)
	tcompare(t, err, u1, User{math.MaxInt32, 1}, "get user")

	err = db.Insert(ctxbg, &User2{math.MaxUint32 + 1, 1})
	tneed(t, err, ErrParam, "out of range")

	err = db.Insert(ctxbg, &User2{0, math.MinInt32 - 1})
	tneed(t, err, ErrParam, "out of range")

	err = db.Insert(ctxbg, &User2{0, math.MaxInt32 + 1})
	tneed(t, err, ErrParam, "out of range")

	u2 := User2{math.MaxUint32, 1}
	err = db.Insert(ctxbg, &u2)
	tcheck(t, err, "insert user")
	err = db.Get(ctxbg, &u2)
	tcompare(t, err, u2, User2{math.MaxUint32, 1}, "get user")
}

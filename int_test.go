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
	os.Remove("testdata/intwidth.db")
	db, err := topen(t, "testdata/intwidth.db", nil, User{}, User2{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	err = db.Insert(&User{math.MinInt32 - 1, 1})
	tneed(t, err, ErrParam, "out of range")

	err = db.Insert(&User{math.MaxInt32 + 1, 1})
	tneed(t, err, ErrParam, "out of range")

	err = db.Insert(&User{0, math.MaxUint32 + 1})
	tneed(t, err, ErrParam, "out of range")

	u0 := User{math.MinInt32, 1}
	err = db.Insert(&u0)
	tcheck(t, err, "insert user")
	err = db.Get(&u0)
	tcheck(t, err, "get user")

	u1 := User{math.MaxInt32, 1}
	err = db.Insert(&u1)
	tcheck(t, err, "insert user")
	err = db.Get(&u1)
	tcheck(t, err, "get user")

	err = db.Insert(&User2{math.MaxUint32 + 1, 1})
	tneed(t, err, ErrParam, "out of range")

	err = db.Insert(&User2{0, math.MinInt32 - 1})
	tneed(t, err, ErrParam, "out of range")

	err = db.Insert(&User2{0, math.MaxInt32 + 1})
	tneed(t, err, ErrParam, "out of range")

	u2 := User2{math.MaxUint32, 1}
	err = db.Insert(&u2)
	tcheck(t, err, "insert user")
	err = db.Get(&u2)
	tcheck(t, err, "get user")
}

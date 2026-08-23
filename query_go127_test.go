//go:build go1.27

package bstore

import (
	"os"
	"testing"
)

func TestQueryDBTx(t *testing.T) {
	type User struct {
		ID   int
		Name string
	}

	const path = "testdata/tmp.querydbtx.db"
	os.Remove(path)
	db, err := topen(t, path, nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	u0 := User{Name: "mjl"}
	err = db.Insert(ctxbg, &u0)
	tcheck(t, err, "insert u0")

	x0, err := db.Query[User](ctxbg).FilterEqual("Name", "mjl").Get()
	tcompare(t, err, x0, u0, "compare")

	err = db.Read(ctxbg, func(tx *Tx) error {
		x1, err := tx.Query[User]().FilterEqual("Name", "mjl").Get()
		tcompare(t, err, x1, u0, "compare")
		return nil
	})
	tcheck(t, err, "tx query")
}

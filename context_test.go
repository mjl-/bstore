package bstore

import (
	"context"
	"os"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestContext(t *testing.T) {
	type X struct {
		ID int
	}

	const path = "testdata/tmp.context.db"
	os.Remove(path)
	db, err := topen(t, path, nil, X{})
	tcheck(t, err, "open")

	err = db.Insert(ctxbg, &X{}, &X{})
	tcheck(t, err, "insert values")

	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	// Early cancel, don't start transaction.
	_, err = db.Begin(canceled, false)
	tneed(t, err, context.Canceled, "begin canceled")

	// Cancel during transaction.
	ctx, cancel := context.WithCancel(context.Background())
	tx, err := db.Begin(ctx, false)
	tcheck(t, err, "begin")
	cancel()
	err = tx.Get(&X{ID: 1})
	tneed(t, err, context.Canceled, "begin and canceled get")
	tx.Rollback()

	// Cancel before transaction.
	err = db.Read(canceled, func(tx *Tx) error {
		return nil
	})
	tneed(t, err, context.Canceled, "read tx canceled")

	// Cancel during transaction.
	ctx, cancel = context.WithCancel(context.Background())
	err = db.Read(ctx, func(tx *Tx) error {
		cancel()
		return tx.Get(&X{ID: 1})
	})
	tneed(t, err, context.Canceled, "read tx and canceled get")

	// Canceled Get
	err = db.Get(canceled, &X{ID: 1})
	tneed(t, err, context.Canceled, "canceled db get")

	// Canceled DB Query
	q := QueryDB[X](canceled, db)
	err = q.Err()
	tneed(t, err, context.Canceled, "canceled db query")

	// DB Query with canceled List
	ctx, cancel = context.WithCancel(context.Background())
	q = QueryDB[X](ctx, db)
	err = q.Err()
	tcheck(t, err, "query error")
	cancel()
	_, err = q.List()
	tneed(t, err, context.Canceled, "canceled db query list")

	// DB Query with cancel during ForEach.
	ctx, cancel = context.WithCancel(context.Background())
	q = QueryDB[X](ctx, db)
	err = q.Err()
	tcheck(t, err, "query error")
	err = q.ForEach(func(x X) error {
		cancel()
		return nil
	})
	tneed(t, err, context.Canceled, "canceled db query foreach")

	// DB Query with cancel after Next.
	ctx, cancel = context.WithCancel(context.Background())
	q = QueryDB[X](ctx, db)
	err = q.Err()
	tcheck(t, err, "query error")
	var id int
	err = q.NextID(&id)
	tcheck(t, err, "next id")
	cancel()
	err = q.NextID(&id)
	tneed(t, err, context.Canceled, "db query next id after cancel")
	q.Close()

	// Canceled Tx Query
	err = db.Read(canceled, func(tx *Tx) error {
		q := QueryTx[X](tx)
		return q.Err()
	})
	tneed(t, err, context.Canceled, "canceled tx query")

	// Tx Query with cancel.
	ctx, cancel = context.WithCancel(context.Background())
	err = db.Read(ctx, func(tx *Tx) error {
		q := QueryTx[X](tx)
		err = q.Err()
		tcheck(t, err, "query error")
		cancel()
		_, err = q.List()
		return err
	})
	tneed(t, err, context.Canceled, "canceled tx query list")

	// Open with context with timeout.
	now := time.Now()
	ctx, cancel = context.WithTimeout(ctxbg, 0)
	defer cancel()
	_, err = Open(ctx, path, nil, X{})
	tneed(t, err, bolt.ErrTimeout, "open with context with exceeded deadline")
	if time.Since(now) > time.Second {
		t.Fatalf("timeout took more than 1 second, should be immediate")
	}

	tclose(t, db)
}

//go:build go1.27

package bstore

import (
	"context"
)

// Query returns a new Query for type T. When an operation on the query is
// executed, a read-only/writable transaction is created as appropriate for the
// operation.
func (db *DB) Query[T any](ctx context.Context) *Query[T] {
	return QueryDB[T](ctx, db)
}

// Query returns a new Query that operates on type T using transaction tx. The
// context of the transaction is used for the query.
func (tx *Tx) Query[T any]() *Query[T] {
	return QueryTx[T](tx)
}

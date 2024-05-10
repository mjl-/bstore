package bstore

import (
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"reflect"
	"testing"
)

func tcheck(t *testing.T, err error, msg string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", msg, err)
	}
}

func tneed(t *testing.T, err error, expErr error, msg string) {
	t.Helper()
	if err == nil || (expErr == ErrAbsent && err != ErrAbsent) || !errors.Is(err, expErr) {
		t.Fatalf("%s: got %q, expected error %q", msg, fmt.Sprintf("%v", err), expErr.Error())
	}
}

func tcompare(t *testing.T, err error, got, exp any, msg string) {
	t.Helper()
	tcheck(t, err, msg)
	if !reflect.DeepEqual(got, exp) {
		t.Fatalf("%s: got:\n%v\nexpected:\n%v", msg, got, exp)
	}
}

func tclose(t *testing.T, db *DB) {
	err := db.Close()
	tcheck(t, err, "close")
}

// pkclone returns a pointer to a new zero struct value of the
// pointer-dereferenced type of struct pointer vp, but with the first field
// copied into the new value.
// Useful for a "Get" after an "Insert", and to compare for equality.
func pkclone(vp any) any {
	rv := reflect.ValueOf(vp).Elem()
	if rv.Kind() != reflect.Struct {
		panic("pkclone: v must be a struct")
	}
	nvp := reflect.New(rv.Type())
	nvp.Elem().Field(0).Set(rv.Field(0))
	return nvp.Interface()
}

func ptr[T any](v T) *T {
	return &v
}

var withReopen bool

func TestMain(m *testing.M) {
	log.SetFlags(0)

	if s := os.Getenv("BSTORE_TEST_LOGLEVEL"); s != "" {
		var level slog.Level
		err := level.UnmarshalText([]byte(s))
		if err != nil {
			fmt.Fprintf(os.Stderr, "parsing level %q from $BSTORE_TEST_LOGLEVEL: %v\n", s, err)
			os.Exit(2)
		}
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
	}

	os.Mkdir("testdata", 0700)

	// We want to run all tests twice: once without reopening the DB and once with
	// reopening. Looks like this works, but the first Run writes the coverage profile.
	// Good enough.
	sanityChecks = true
	withReopen = true
	e := m.Run()
	if e > 0 {
		os.Exit(e)
	}
	sanityChecks = false
	withReopen = false
	os.Exit(m.Run())
}

// To open a db, we open topen instead of Open. topen opens the db twice with same
// types, and verifies no new typeversions are created the second time. This
// leverages all test cases for this check.
func topen(t *testing.T, path string, opts *Options, typeValues ...any) (*DB, error) {
	t.Helper()
	if opts == nil {
		opts = &Options{}
	}
	if opts.RegisterLogger == nil {
		opts.RegisterLogger = slog.Default()
	}
	db, err := Open(ctxbg, path, opts, typeValues...)
	if !withReopen || err != nil {
		return db, err
	}

	oversions := map[string]uint32{}
	for tname, st := range db.typeNames {
		oversions[tname] = st.Current.Version
	}

	tclose(t, db)

	db, err = Open(ctxbg, path, opts, typeValues...)
	tcheck(t, err, "open again")

	nversions := map[string]uint32{}
	for tname, st := range db.typeNames {
		nversions[tname] = st.Current.Version
	}

	if !reflect.DeepEqual(oversions, nversions) {
		t.Fatalf("reopen of db created new typeversions: old %v, new %v", oversions, nversions)
	}

	return db, err
}

type bm struct {
	value string
}

func (b bm) MarshalBinary() ([]byte, error) {
	return []byte(b.value), nil
}

func (b *bm) UnmarshalBinary(buf []byte) error {
	b.value = string(buf)
	return nil
}

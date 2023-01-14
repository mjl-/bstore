package bstore

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io/fs"
	"log"
	mathrand "math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
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
		t.Fatalf("%s: got %v, expect %v", msg, got, exp)
	}
}

func tclose(t *testing.T, db *DB) {
	err := db.Close()
	tcheck(t, err, "close")
}

var withReopen bool

func TestMain(m *testing.M) {
	log.SetFlags(0)

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
	db, err := Open(path, opts, typeValues...)
	if !withReopen || err != nil {
		return db, err
	}

	oversions := map[string]uint32{}
	for tname, st := range db.typeNames {
		oversions[tname] = st.Current.Version
	}

	tclose(t, db)

	db, err = Open(path, opts, typeValues...)
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

func TestOpenOptions(t *testing.T) {
	type User struct {
		ID int
	}

	path := "testdata/openoptions.db"
	os.Remove(path)

	_, err := Open(path, &Options{MustExist: true}, User{})
	tneed(t, err, fs.ErrNotExist, "open with MustExist on absent file")

	db, err := Open(path, &Options{Perm: 0700}, User{})
	tcheck(t, err, "open")
	fi, err := os.Stat(path)
	tcheck(t, err, "stat")
	if fi.Mode()&fs.ModePerm != 0700 {
		t.Fatalf("mode of new db is %o, expected 0700", fi.Mode()&fs.ModePerm)
	}

	// not closing DB yet, for timeout

	_, err = Open(path, &Options{Timeout: time.Second}, User{})
	tneed(t, err, bolt.ErrTimeout, "open with timeout")

	tclose(t, db)

	db, err = Open(path, &Options{MustExist: true}, User{})
	tcheck(t, err, "open with MustExist")
	tclose(t, db)
}

func TestStore(t *testing.T) {
	type Sub struct {
		ID    int
		Email string
	}

	type Mapkey struct {
		K1 int
		K2 string
	}
	type Mapvalue struct {
		Data []byte
		Time *time.Time
	}

	type User struct {
		ID         int64
		Name       string    `bstore:"unique"`
		Registered time.Time `bstore:"nonzero"`

		Byte    byte
		Int8    int8
		Int16   int16
		Int32   int32
		Int64   int64
		Uint64  uint64
		Float32 float32
		Float64 float64
		String  string
		Bytes   []byte
		Struct  Sub
		Slice   []string
		Map     map[string]struct{}
		Map2    map[Mapkey]Mapvalue
		Time    time.Time

		Byteptr    *byte
		Int8ptr    *int8
		Int16ptr   *int16
		Int32ptr   *int32
		Int64ptr   *int64
		Uint64ptr  *uint64
		Float32ptr *float32
		Float64ptr *float64
		Stringptr  *string
		Bytesptr   *[]byte
		Structptr  *Sub
		Sliceptr   *[]string
		Mapptr     *map[string]struct{}
		Map2ptr    *map[Mapkey]Mapvalue
		Timeptr    *time.Time

		Ignore  int `bstore:"-"`
		private int
	}

	tv, err := gatherTypeVersion(reflect.TypeOf(User{}))
	tcheck(t, err, "gatherinTypeVersions")
	if tv.name != "User" {
		t.Fatalf("name %q, expected User", tv.name)
	}
	tv.Version = 1
	st := storeType{tv.name, reflect.TypeOf(User{}), tv, map[uint32]*typeVersion{tv.Version: tv}}
	now := time.Now().Round(0) // time without monotonic time, for later deepequal comparison
	u := User{
		ID:         123,
		Name:       "mjl",
		Registered: now,
		Ignore:     2,
		private:    4,
	}
	ubuf, err := st.pack(reflect.ValueOf(u))
	tcheck(t, err, "pack")
	err = st.parse(reflect.ValueOf(&User{}).Elem(), ubuf)
	tcheck(t, err, "parse")

	os.Remove("testdata/test.db")
	db, err := topen(t, "testdata/test.db", nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	err = db.Write(func(tx *Tx) error {
		nu := User{Registered: now}
		err := tx.Insert(&nu)
		tcheck(t, err, "insert new user")
		if nu.ID == 0 {
			t.Fatalf("no id assigned for new record")
		}

		u = User{
			ID:         123,
			Name:       "mjl",
			Registered: now,

			Byte:    'a',
			Int8:    20,
			Int16:   1000,
			Int32:   1 << 20,
			Int64:   -1,
			Uint64:  ^uint64(0),
			Float32: 1.123,
			Float64: 0.000001,
			String:  "test",
			Bytes:   []byte("hi"),
			Struct:  Sub{1, "user@example.org"},
			Slice:   []string{"a", "b"},
			Time:    now,
			Map:     map[string]struct{}{"a": {}},
			Map2: map[Mapkey]Mapvalue{
				{1, "a"}: {[]byte("test"), nil},
				{1, "b"}: {nil, &now},
				{2, "a"}: {nil, nil},
				{3, "d"}: {[]byte("hi"), &now},
			},
		}
		err = tx.Insert(&u)
		tcheck(t, err, "insert user")

		err = tx.Insert(&u)
		tneed(t, err, ErrUnique, "insert dup")
		tcompare(t, nil, u.ID, int64(123), "id unchanged")

		uptrs := User{
			Name:       "mjl2",
			Registered: now,

			Byteptr:    &u.Byte,
			Int8ptr:    &u.Int8,
			Int16ptr:   &u.Int16,
			Int32ptr:   &u.Int32,
			Int64ptr:   &u.Int64,
			Uint64ptr:  &u.Uint64,
			Float32ptr: &u.Float32,
			Float64ptr: &u.Float64,
			Stringptr:  &u.String,
			Bytesptr:   &u.Bytes,
			Structptr:  &u.Struct,
			Sliceptr:   &u.Slice,
			Mapptr:     &u.Map,
			Map2ptr:    &u.Map2,
			Timeptr:    &u.Time,
		}
		err = tx.Insert(&uptrs)
		tcheck(t, err, "inserting user with pointers")

		u2 := User{ID: u.ID + 999}
		err = tx.Get(&u2)
		tneed(t, err, ErrAbsent, "fetching non-existing id")

		err = tx.Get(nil)
		tneed(t, err, ErrParam, "nil get")

		err = tx.Insert(nil)
		tneed(t, err, ErrParam, "nil insert")

		u.Name = "nmjl"
		err = tx.Update(&u)
		tcheck(t, err, "update")

		err = tx.Update(&User{ID: u.ID + 999})
		tneed(t, err, ErrAbsent, "update on absent record")

		err = tx.Update(nil)
		tneed(t, err, ErrParam, "nil update")

		u2 = User{ID: u.ID}
		err = tx.Get(&u2)
		tcheck(t, err, "get")
		if u2.Name != "nmjl" {
			t.Fatalf("save did not update")
		}

		nuptrs := User{ID: uptrs.ID}
		err = tx.Get(&nuptrs)
		tcheck(t, err, "get of user with pointers")
		if !reflect.DeepEqual(uptrs, nuptrs) {
			log.Printf("uptrs: %v", uptrs)
			log.Printf("nuptrs: %v", nuptrs)
			t.Fatalf("uptrs and nuptrs not equal")
		}

		err = tx.Delete(&u)
		tcheck(t, err, "delete")

		err = tx.Delete(&u)
		tneed(t, err, ErrAbsent, "deleting non-existing id")

		err = tx.Delete(nil)
		tneed(t, err, ErrParam, "nil delete")

		err = tx.Delete((*User)(nil))
		tneed(t, err, ErrParam, "nil delete ptr")

		err = tx.Delete("bogus")
		tneed(t, err, ErrParam, "delete on non-struct/structptr")

		u3 := User{Name: "unique1"}
		err = tx.Insert(&u3)
		tneed(t, err, ErrZero, "inserting with zero value")
		tcompare(t, nil, u3.ID, int64(0), "zero id")

		return nil
	})
	tcheck(t, err, "write")

	n, err := QueryDB[User](db).Count()
	tcheck(t, err, "count")
	if n != 2 {
		t.Fatalf("got %d records, expected 2", n)
	}
}

func TestRegister(t *testing.T) {
	type OK struct {
		ID int
	}
	type Empty struct {
	}
	type Private1 struct {
		id int // First field must be PK.
		ID int
	}
	type Private2 struct {
		ID int `bstore:"-"` // Cannot skip first field.
	}
	type Noauto struct {
		ID   int
		Text string `bstore:"noauto"` // noauto is only for PK.
	}
	type Badtag struct {
		ID int `bstore:"bogus"`
	}
	type Badindex1 struct {
		ID   int
		Name string `bstore:"index ID"` // Must start with own field.
	}
	type Badindex2 struct {
		ID   int
		Name string `bstore:"index Name Name Superfluous"` // One word too many.
	}
	type Badindex3 struct {
		ID   int
		Name string `bstore:"index Name+Absent"` // Field does not exist.
	}
	type BadPKPtr struct {
		ID *int
	}
	type BadIndexPtr struct {
		ID   int
		Name *string `bstore:"index"`
	}
	type BadIndexDupfield struct {
		ID   int
		Name *string `bstore:"index Name+Name"`
	}
	type BadPKNonzero struct {
		ID int `bstore:"nonzero"` // Superfluous nonzero tag.
	}
	type BadRefA struct {
		ID  int
		XID int `bstore:"ref BadRefB,ref BadRefB"`
	}
	type BadRefB struct {
		ID int
	}
	type Embed struct {
		ID int
	}
	type BadEmbed struct {
		Embed
	}
	type BadEmbedNonzero struct {
		PK    int
		Embed `bstore:"nonzero"`
	}
	type BadEmbedIgnore struct {
		PK    int
		Embed `bstore:"-"`
	}
	type BadName struct {
		ID int `bstore:"name "`
	}
	type BadTypeDefault struct {
		ID    int
		Field Embed `bstore:"default xyz"`
	}
	type Badptrptr struct {
		ID     int
		Ptrptr **int
	}
	type Badptrptrslice struct {
		ID    int
		Slice []**int
	}
	type Badptrptrstruct struct {
		ID     int
		Struct **Embed
	}
	type Badptrptrmap2 struct {
		ID  int
		Map map[int]**int
	}
	type Badptrmapkey struct {
		ID  int
		Map map[*int]int
	}

	path := "testdata/register.db"
	os.Remove(path)

	_, err := topen(t, path, nil, "not a struct")
	tneed(t, err, ErrParam, "bad type")

	_, err = topen(t, path, nil, OK{}, OK{})
	tneed(t, err, ErrParam, "duplicate name")

	_, err = topen(t, path, nil, &OK{})
	tneed(t, err, ErrParam, "pointer")

	_, err = topen(t, path, nil, Empty{})
	tneed(t, err, ErrType, "no PK")

	_, err = topen(t, path, nil, struct{}{})
	tneed(t, err, ErrType, "no PK")

	_, err = topen(t, path, nil, Private1{id: 1})
	tneed(t, err, ErrType, "bad Private1")

	_, err = topen(t, path, nil, Private2{})
	tneed(t, err, ErrType, "bad Private2")

	_, err = topen(t, path, nil, Noauto{})
	tneed(t, err, ErrType, "bad Noauto")

	_, err = topen(t, path, nil, Badtag{})
	tneed(t, err, ErrType, "bad Badtag")

	_, err = topen(t, path, nil, Badindex1{})
	tneed(t, err, ErrType, "bad Badindex1")

	_, err = topen(t, path, nil, Badindex2{})
	tneed(t, err, ErrType, "bad Badindex2")

	_, err = topen(t, path, nil, Badindex3{})
	tneed(t, err, ErrType, "bad Badindex3")

	_, err = topen(t, path, nil, BadPKPtr{})
	tneed(t, err, ErrType, "bad BadPKPtr")

	_, err = topen(t, path, nil, BadIndexPtr{})
	tneed(t, err, ErrType, "bad BadIndexPtr")

	_, err = topen(t, path, nil, BadIndexDupfield{})
	tneed(t, err, ErrType, "bad BadIndexDupfield")

	_, err = topen(t, path, nil, BadPKNonzero{})
	tneed(t, err, ErrType, "bad BadPKNonzero")

	_, err = topen(t, path, nil, BadRefA{}, BadRefB{})
	tneed(t, err, ErrType, "bad dup ref")

	_, err = topen(t, path, nil, BadEmbed{})
	tneed(t, err, ErrType, "bad embed for Pk")

	_, err = topen(t, path, nil, BadEmbedNonzero{})
	tneed(t, err, ErrType, "bad nonzero tag on embed")

	_, err = topen(t, path, nil, BadEmbedIgnore{})
	tneed(t, err, ErrType, "bad ignore for embed")

	_, err = topen(t, path, nil, BadName{})
	tneed(t, err, ErrType, "bad name on field")

	_, err = topen(t, path, nil, BadTypeDefault{})
	tneed(t, err, ErrType, "bad type with default")

	_, err = topen(t, path, nil, Badptrptr{})
	tneed(t, err, ErrType, "bad type with ptr to ptr")

	_, err = topen(t, path, nil, Badptrptrslice{})
	tneed(t, err, ErrType, "bad type with ptr to ptr")

	_, err = topen(t, path, nil, Badptrptrstruct{})
	tneed(t, err, ErrType, "bad type with ptr to ptr")

	_, err = topen(t, path, nil, Badptrptrmap2{})
	tneed(t, err, ErrType, "bad type with ptr to ptr")

	_, err = topen(t, path, nil, Badptrmapkey{})
	tneed(t, err, ErrType, "bad type with ptr key in map")
}

func TestUnique(t *testing.T) {
	type User struct {
		ID   int
		Name string `bstore:"unique"`
	}

	os.Remove("testdata/unique.db")
	db, err := topen(t, "testdata/unique.db", nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	err = db.Write(func(tx *Tx) error {
		a := User{Name: "a"}
		b := User{Name: "b"}
		dup := User{Name: "a"}

		err := tx.Insert(&a)
		tcheck(t, err, "insert")

		err = tx.Insert(&b)
		tcheck(t, err, "insert")

		err = tx.Insert(&dup)
		tneed(t, err, ErrUnique, "inserting with existing key")

		err = tx.Delete(a)
		tcheck(t, err, "delete")

		// No longer duplicate.
		err = tx.Insert(&dup)
		tcheck(t, err, "insert dup")

		b.Name = "a"
		err = tx.Update(&b)
		tneed(t, err, ErrUnique, "updating with existing key")

		err = tx.Insert(&User{Name: "test\u0000"})
		tneed(t, err, ErrParam, "cannot have string with zero byte")

		return nil
	})
	tcheck(t, err, "db update")
}

func TestReference(t *testing.T) {
	type User struct {
		ID      int
		Name    string
		GroupID int `bstore:"ref Group"`
	}
	type Group struct {
		ID   int
		Name string
	}

	type Group2 struct {
		ID   uint `bstore:"typename Group"`
		Name string
	}

	os.Remove("testdata/reference.db")

	_, err := topen(t, "testdata/reference.db", nil, User{})
	tneed(t, err, ErrType, "missing reference type")

	_, err = topen(t, "testdata/reference.db", nil, User{}, Group2{})
	tneed(t, err, ErrType, "reference field type mismatch")

	db, err := topen(t, "testdata/reference.db", nil, User{}, Group{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	err = db.Write(func(tx *Tx) error {
		a := User{}

		// This is fine because GroupID has the zero value.
		err := tx.Insert(&a)
		tcheck(t, err, "insert")

		err = tx.Insert(&User{GroupID: 999})
		tneed(t, err, ErrReference, "inserting with reference to non-existing id")

		err = tx.Insert(&Group{ID: 999})
		tcheck(t, err, "insert group")

		// Same user insert as before, now it is valid.
		u := &User{GroupID: 999}
		err = tx.Insert(u)
		tcheck(t, err, "insert user")

		// Cannot remove group yet, user is still referencing it.
		err = tx.Delete(&Group{ID: 999})
		tneed(t, err, ErrReference, "removing record that is still referenced")

		err = tx.Delete(u)
		tcheck(t, err, "delete user")

		err = tx.Delete(&Group{ID: 999})
		tcheck(t, err, "delete group")

		return nil
	})
	tcheck(t, err, "db update")
}

func TestCreateIndex(t *testing.T) {
	type User struct {
		ID   int
		Name string
	}

	// Now with index.
	type User2 struct {
		ID   int    `bstore:"typename User"`
		Name string `bstore:"unique"`
	}

	os.Remove("testdata/createindex.db")
	db, err := topen(t, "testdata/createindex.db", nil, User{})
	tcheck(t, err, "open")

	u0 := User{Name: "a"}
	u1 := User{Name: "a"}
	err = db.Write(func(tx *Tx) error {
		err := tx.Insert(&u0)
		tcheck(t, err, "insert a")

		err = tx.Insert(&u1)
		tcheck(t, err, "insert a")

		return nil
	})
	tcheck(t, err, "write")
	tclose(t, db)

	_, err = topen(t, "testdata/createindex.db", nil, User2{})
	tneed(t, err, ErrUnique, "open with new unique index with duplicate value")

	os.Remove("testdata/createindex.db")
	db, err = topen(t, "testdata/createindex.db", nil, User{})
	tcheck(t, err, "open")

	u0 = User{Name: "a"}
	u1 = User{Name: "b"}
	err = db.Insert(&u0)
	tcheck(t, err, "insert u0")
	err = db.Insert(&u1)
	tcheck(t, err, "insert u1")

	tclose(t, db)

	db, err = topen(t, "testdata/createindex.db", nil, User2{})
	tcheck(t, err, "open")

	var ids []int
	err = QueryDB[User2](db).FilterNonzero(User2{Name: "a"}).IDs(&ids)
	tcompare(t, err, ids, []int{u0.ID}, "list")

	tclose(t, db)
}

func TestTypeVersions(t *testing.T) {
	type User struct {
		ID   int
		Name string
	}

	type User2 struct {
		ID   int    `bstore:"typename User"`
		Name string `bstore:"unique"`
	}

	type User3 struct {
		ID   int    `bstore:"typename User"`
		Name string `bstore:"nonzero,unique"`
	}

	type User4 struct {
		ID   int    `bstore:"typename User"`
		Name string `bstore:"nonzero,unique"`
		Age  int
	}

	type User5 struct {
		ID   int    `bstore:"typename User"`
		Name string `bstore:"default test"`
	}

	type User6 struct {
		ID      int    `bstore:"typename User"`
		Name    string `bstore:"default test"`
		GroupID int    `bstore:"ref Group"`
	}

	type Group struct {
		ID int
	}

	os.Remove("testdata/typeversions.db")
	db, err := topen(t, "testdata/typeversions.db", nil, User{})
	tcheck(t, err, "open")

	reopen := func(v ...any) {
		t.Helper()

		var err error
		tclose(t, db)

		// Register a different type. Insert the other type. List the two records with different typeVersions.
		db, err = topen(t, "testdata/typeversions.db", nil, v...)
		tcheck(t, err, "open")
	}

	checkTypes := func(exp string) {
		err := db.bdb.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("User"))
			if b == nil {
				t.Fatalf("missing bucket User")
			}
			tb := b.Bucket([]byte("types"))
			if tb == nil {
				t.Fatalf("missing types bucket")
			}
			var versions []string
			err := tb.ForEach(func(bk, bv []byte) error {
				version := binary.BigEndian.Uint32(bk)
				versions = append(versions, fmt.Sprintf("%d", version))
				return nil
			})
			tcheck(t, err, "reading types")
			if strings.Join(versions, ",") != exp {
				t.Fatalf(`got typeversions %q, expected %q`, strings.Join(versions, ","), exp)
			}
			return nil
		})
		tcheck(t, err, "view")
	}

	checkTypes("1")

	reopen(User{})
	checkTypes("1")

	u := User{Name: "hi"}
	err = db.Insert(&u)
	tcheck(t, err, "insert user")

	reopen(User2{})
	checkTypes("1,2")
	err = db.Write(func(tx *Tx) error {
		u := User2{Name: "hi2"}
		if err := tx.Insert(&u); err != nil {
			t.Fatalf("inserting user: %v", err)
		}

		_, err := QueryTx[User2](tx).List()
		tcheck(t, err, "list")
		return nil
	})
	tcheck(t, err, "insert/list second user")

	reopen(User{})
	checkTypes("1,2,3")

	reopen(User2{})
	checkTypes("1,2,3,4")

	_, err = QueryDB[User2](db).List()
	tcheck(t, err, "all")

	reopen(User3{})
	checkTypes("1,2,3,4,5")

	reopen(User4{})
	checkTypes("1,2,3,4,5,6")

	reopen(User{})
	checkTypes("1,2,3,4,5,6,7")

	reopen(User5{})
	checkTypes("1,2,3,4,5,6,7,8")

	reopen(User6{}, Group{})
	checkTypes("1,2,3,4,5,6,7,8,9")

	tclose(t, db)
}

func TestInsertSeqdup(t *testing.T) {
	type User struct {
		ID int
	}
	os.Remove("testdata/insertseqdup.db")
	db, err := topen(t, "testdata/insertseqdup.db", nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	err = db.Write(func(tx *Tx) error {
		u1 := User{ID: int(tx.btx.Bucket([]byte("User")).Bucket([]byte("records")).Sequence() + 1)}
		err := tx.Insert(&u1)
		tcheck(t, err, "insert")

		u2 := User{}
		err = tx.Insert(&u2)
		// This now works because we increase the sequence number when user inserts a higher id then current sequence.
		tcheck(t, err, "inserting record where next sequence seems already present")

		return nil
	})
	tcheck(t, err, "write")
}

func TestRemoveNoautoSeq(t *testing.T) {
	type User struct {
		ID int `bstore:"noauto"`
	}
	type User2 struct {
		ID int `bstore:"typename User"`
	}
	os.Remove("testdata/removenoautoseq.db")

	db, err := topen(t, "testdata/removenoautoseq.db", nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	var u0, u1 User
	var seq0 int
	err = db.Write(func(tx *Tx) error {
		err := tx.btx.Bucket([]byte("User")).Bucket([]byte("records")).SetSequence(1)
		tcheck(t, err, "setsequence")
		seq0 = 1

		u0 = User{ID: seq0}
		u1 = User{ID: seq0 + 1}
		err = tx.Insert(&u0, &u1)
		tcheck(t, err, "insert")

		return nil
	})
	tcheck(t, err, "write")

	tclose(t, db)

	// Reopening with User2 (without "noauto") should set the sequence past the highest sequence.
	db, err = topen(t, "testdata/removenoautoseq.db", nil, User2{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	var seq1 int
	err = db.Write(func(tx *Tx) error {
		seq1 = int(tx.btx.Bucket([]byte("User")).Bucket([]byte("records")).Sequence())

		if seq1 == seq0 {
			t.Fatalf("sequence did not change %d", seq1)
		}

		err := tx.Insert(&User2{})
		tcheck(t, err, "insert")

		return nil
	})
	tcheck(t, err, "write")
}

func TestPtrZero(t *testing.T) {
	type Sub struct {
		Name string
	}
	type User struct {
		ID     int
		String *string
		Sub    *Sub
	}
	os.Remove("testdata/ptrzero.db")
	db, err := topen(t, "testdata/ptrzero.db", nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	err = db.Write(func(tx *Tx) error {
		u0 := User{}
		err := tx.Insert(&u0)
		tcheck(t, err, "insert")

		var str string
		var sub Sub
		u1 := User{String: &str, Sub: &sub}
		err = tx.Insert(&u1)
		tcheck(t, err, "insert")

		s2 := "hi"
		sub2 := Sub{Name: "hi"}
		u2 := User{String: &s2, Sub: &sub2}
		err = tx.Insert(&u2)
		tcheck(t, err, "insert")

		x0 := User{ID: u0.ID}
		err = tx.Get(&x0)
		tcheck(t, err, "get")

		x1 := User{ID: u1.ID}
		err = tx.Get(&x1)
		tcheck(t, err, "get")

		x2 := User{ID: u2.ID}
		err = tx.Get(&x2)
		tcheck(t, err, "get")

		if !reflect.DeepEqual(u0, x0) {
			t.Fatalf("u0 %v not equal to x0 %v", u0, x0)
		}

		if !reflect.DeepEqual(u1, x1) {
			t.Fatalf("u1 %v not equal to x1 %v", u1, x1)
		}

		if !reflect.DeepEqual(u2, x2) {
			t.Fatalf("u2 %v not equal to x2 %v", u2, x2)
		}

		return nil
	})
	tcheck(t, err, "write")
}

func TestIDTypes(t *testing.T) {
	type Bytes struct {
		ID []byte
	}
	type String struct {
		ID string
	}
	type Uint8 struct {
		ID uint8
	}

	os.Remove("testdata/idtypes.db")
	db, err := topen(t, "testdata/idtypes.db", nil, Bytes{}, String{}, Uint8{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	err = db.Write(func(tx *Tx) error {
		// Bytes
		b := Bytes{ID: []byte("hi")}
		err := tx.Insert(&b)
		tcheck(t, err, "insert")

		xb := Bytes{b.ID}
		err = tx.Get(&xb)
		tcheck(t, err, "get")

		err = tx.Insert(&Bytes{})
		tneed(t, err, ErrZero, "attempt to insert zero key")

		// String
		s := String{"hi"}
		err = tx.Insert(&s)
		tcheck(t, err, "insert")

		xs := String{s.ID}
		err = tx.Get(&xs)
		tcheck(t, err, "get")

		err = tx.Insert(&String{})
		tneed(t, err, ErrZero, "attempt to insert zero key")

		// Uint8
		i := Uint8{}
		err = tx.Insert(&i)
		tcheck(t, err, "insert")

		xi := Uint8{i.ID}
		err = tx.Get(&xi)
		tcheck(t, err, "get")

		for {
			i := Uint8{}
			err = tx.Insert(&i)
			tcheck(t, err, "insert")
			if i.ID == 255 {
				break
			}
		}

		err = tx.Insert(&Uint8{})
		tneed(t, err, ErrSeq, "inserting without next sequence available")

		return nil
	})
	tcheck(t, err, "write")
}

func TestChangeIndex(t *testing.T) {
	type User struct {
		ID        int
		Firstname string `bstore:"index Firstname myidx"`
		Lastname  string
	}
	type User2 struct {
		ID        int `bstore:"typename User"`
		Firstname string
		Lastname  string `bstore:"index Lastname myidx"`
	}
	os.Remove("testdata/changeindex.db")
	db, err := topen(t, "testdata/changeindex.db", nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	err = db.Write(func(tx *Tx) error {
		u := User{0, "a", "b"}
		err := tx.Insert(&u)
		tcheck(t, err, "insert")

		u = User{0, "x", "y"}
		err = tx.Insert(&u)
		tcheck(t, err, "insert")

		return nil
	})
	tcheck(t, err, "write")
	tclose(t, db)

	db, err = topen(t, "testdata/changeindex.db", nil, User2{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	n, err := QueryDB[User2](db).FilterEqual("Firstname", "x").Count()
	tcompare(t, err, n, 1, "count")

	n, err = QueryDB[User2](db).FilterEqual("Firstname", "a").Count()
	tcompare(t, err, n, 1, "count")
}

// Check that indices are empty again after removing all records.
func TestEmptyIndex(t *testing.T) {
	type User struct {
		ID   int
		Name string    `bstore:"unique,index Name+Time"`
		Time time.Time `bstore:"default now,index"`
	}

	path := "testdata/emptyindex.db"
	os.Remove(path)
	db, err := topen(t, path, nil, User{})
	tcheck(t, err, "open")

	err = db.Insert(&User{Name: "a"}, &User{Name: "b"}, &User{Name: "c"})
	tcheck(t, err, "insert")

	n, err := QueryDB[User](db).Count()
	tcompare(t, err, n, 3, "count")

	n, err = QueryDB[User](db).Delete()
	tcompare(t, err, n, 3, "delete all")

	tclose(t, db)

	bdb, err := bolt.Open(path, 0600, nil)
	tcheck(t, err, "bolt open")

	names := []string{
		"Name",
		"Name+Time",
		"Time",
	}
	err = bdb.View(func(btx *bolt.Tx) error {
		b := btx.Bucket([]byte("User"))
		if b == nil {
			t.Fatalf("missing bucket for type User")
		}
		for _, name := range names {
			ib := b.Bucket([]byte("index." + name))
			if ib == nil {
				t.Fatalf("missing index bucket for %q", name)
			}
			k, v := ib.Cursor().First()
			if k != nil {
				t.Fatalf("index %q is not empty after removing all data: %x %x", name, k, v)
			}
		}
		return nil
	})
	tcheck(t, err, "view")
}

// Schema upgrade for which index is dropped.
func TestIndexDrop(t *testing.T) {
	type User struct {
		ID   int
		Name string `bstore:"index"`
	}
	type User2 struct {
		ID   int `bstore:"typename User"`
		Name string
	}

	path := "testdata/indexdrop.db"
	os.Remove(path)
	db, err := topen(t, path, nil, User{})
	tcheck(t, err, "open")

	u := User{Name: "x"}
	err = db.Insert(&u)
	tcheck(t, err, "insert")

	tclose(t, db)

	db, err = topen(t, path, nil, User2{})
	tcheck(t, err, "open")

	_, err = QueryDB[User2](db).FilterNonzero(User2{Name: "x"}).Get()
	tcheck(t, err, "get by name")
}

// Schema upgrade for which index needs to be recreated for wider keys.
func TestIndexWiden(t *testing.T) {
	type User struct {
		ID  int
		Num int16 `bstore:"index"`
	}
	type User2 struct {
		ID  int   `bstore:"typename User"`
		Num int32 `bstore:"index"`
	}

	path := "testdata/indexwiden.db"
	os.Remove(path)
	db, err := topen(t, path, nil, User{})
	tcheck(t, err, "open")

	u := User{Num: 10}
	err = db.Insert(&u)
	tcheck(t, err, "insert")

	_, err = QueryDB[User](db).FilterNonzero(User{Num: 10}).Get()
	tcheck(t, err, "get by num")

	tclose(t, db)

	db, err = topen(t, path, nil, User2{})
	tcheck(t, err, "open")

	_, err = QueryDB[User2](db).FilterNonzero(User2{Num: 10}).Get()
	tcheck(t, err, "get by num")
}

// Schema upgrade with new nonzero field while data is present.
func TestNewNonzero(t *testing.T) {
	type User struct {
		ID int
	}
	type User2 struct {
		ID   int    `bstore:"typename User"`
		Name string `bstore:"nonzero"`
	}

	path := "testdata/newnonzero.db"
	os.Remove(path)
	db, err := topen(t, path, nil, User{})
	tcheck(t, err, "open")

	u := User{}
	err = db.Insert(&u)
	tcheck(t, err, "insert")

	_, err = QueryDB[User](db).Get()
	tcheck(t, err, "get")

	tclose(t, db)

	_, err = topen(t, path, nil, User2{})
	tneed(t, err, ErrZero, "new field with nonzero for non-empty table")
}

func TestNonzero(t *testing.T) {
	type Nonzero[T any] struct {
		ID      int
		Nonzero T `bstore:"nonzero"`
	}

	type Nz struct {
		V int `bstore:"nonzero"`
	}

	path := "testdata/nonzero.db"

	tnonzero := func(exp error, val any) {
		t.Helper()
		os.Remove(path)
		db, err := topen(t, path, nil, reflect.ValueOf(val).Elem().Interface())
		tcheck(t, err, "open")
		err = db.Insert(val)
		if exp != nil && (err == nil || !errors.Is(err, exp)) {
			t.Fatalf("got err %q, expected %v", err, exp)
		} else if exp == nil && err != nil {
			t.Fatalf("got err %q, expected nil", err)
			err = db.Get(val)
			tcheck(t, err, "get")
		}
		tclose(t, db)
	}

	tnonzero(ErrZero, &Nonzero[int]{0, 0})
	tnonzero(ErrZero, &Nonzero[int8]{0, 0})
	tnonzero(ErrZero, &Nonzero[int16]{0, 0})
	tnonzero(ErrZero, &Nonzero[int32]{0, 0})
	tnonzero(ErrZero, &Nonzero[int64]{0, 0})
	tnonzero(ErrZero, &Nonzero[string]{0, ""})
	tnonzero(ErrZero, &Nonzero[float32]{0, 0})
	tnonzero(ErrZero, &Nonzero[[]byte]{0, nil})
	tnonzero(ErrZero, &Nonzero[[]Nz]{0, []Nz{{0}}})
	tnonzero(ErrZero, &Nonzero[[]Nz]{0, []Nz(nil)})
	tnonzero(nil, &Nonzero[[]Nz]{0, []Nz{}})
	tnonzero(nil, &Nonzero[[]Nz]{0, []Nz{{1}}})
	tnonzero(nil, &Nonzero[[]*Nz]{0, []*Nz{nil}})
	tnonzero(ErrZero, &Nonzero[Nz]{0, Nz{0}})
	tnonzero(nil, &Nonzero[Nz]{0, Nz{1}})
	tnonzero(ErrZero, &Nonzero[*Nz]{0, (*Nz)(nil)})
	tnonzero(ErrZero, &Nonzero[*Nz]{0, &Nz{0}})
	tnonzero(nil, &Nonzero[*Nz]{0, &Nz{1}})
	tnonzero(ErrZero, &Nonzero[map[Nz]Nz]{0, nil})
	tnonzero(nil, &Nonzero[map[Nz]Nz]{0, map[Nz]Nz{}})
	tnonzero(ErrZero, &Nonzero[map[Nz]Nz]{0, map[Nz]Nz{{1}: {0}}})
	tnonzero(ErrZero, &Nonzero[map[Nz]Nz]{0, map[Nz]Nz{{0}: {1}}})
	tnonzero(nil, &Nonzero[map[Nz]Nz]{0, map[Nz]Nz{{1}: {1}}})
	tnonzero(nil, &Nonzero[map[Nz]*Nz]{0, map[Nz]*Nz{{1}: nil}})
	tnonzero(ErrZero, &Nonzero[map[Nz]*Nz]{0, map[Nz]*Nz{{1}: {0}}})
	tnonzero(nil, &Nonzero[map[Nz]*Nz]{0, map[Nz]*Nz{{1}: nil}})
}

func TestRefIndexConflict(t *testing.T) {
	type User struct {
		ID      int
		Age     int `bstore:"index Age GroupID:Group"` // Conflicting name with automatic index on GroupID.
		GroupID int `bstore:"ref Group"`
	}
	type Group struct {
		ID int
	}

	path := "testdata/refindexconflict.db"
	os.Remove(path)
	_, err := topen(t, path, nil, User{}, Group{})
	tneed(t, err, ErrType, "open")
}

func TestIndexRemain(t *testing.T) {
	// We'll remove type User, taking a reference to an index out of Group, but leaving Groups index "Name".
	type User struct {
		ID      int
		GroupID int `bstore:"ref Group"` // Automatic index.
	}
	type Group struct {
		ID   int
		Name string `bstore:"unique"`
	}

	path := "testdata/indexremain.db"
	os.Remove(path)
	db, err := topen(t, path, nil, User{}, Group{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	err = db.Drop("User")
	tcheck(t, err, "drop")
}

func TestChangeNonzero(t *testing.T) {
	type Sub struct {
		Name string
	}
	type Key struct {
		ID int
	}
	type Value struct {
		Value []byte
	}
	type Elem struct {
		ID int
	}
	type User struct {
		ID     int
		Name   string
		Int    int
		Bytes  []byte
		Struct Sub
		Map    map[Key]Value
		Slice  []Elem
	}

	type Sub2 struct {
		Name string `bstore:"nonzero"`
	}
	type Key2 struct {
		ID int `bstore:"nonzero"`
	}
	type Value2 struct {
		Value []byte `bstore:"nonzero"`
	}
	type Elem2 struct {
		ID int `bstore:"nonzero"`
	}
	type User2 struct {
		ID     int             `bstore:"typename User"`
		Name   string          `bstore:"nonzero"`
		Int    int             `bstore:"nonzero"`
		Bytes  []byte          `bstore:"nonzero"`
		Struct Sub2            `bstore:"nonzero"`
		Map    map[Key2]Value2 `bstore:"nonzero"`
		Slice  []Elem2         `bstore:"nonzero"`
	}

	clone := func(v User2) User {
		var b bytes.Buffer
		err := gob.NewEncoder(&b).Encode(v)
		tcheck(t, err, "encode")
		var n User
		err = gob.NewDecoder(&b).Decode(&n)
		tcheck(t, err, "decode")
		return n
	}

	testValue := func(good User, bad User2) {
		t.Helper()

		os.Remove("testdata/changenonzero.db")
		db, err := topen(t, "testdata/changenonzero.db", nil, User{})
		tcheck(t, err, "open")

		err = db.Insert(&good)
		tcheck(t, err, "insert good user")

		tclose(t, db)
		db, err = topen(t, "testdata/changenonzero.db", nil, User2{})
		tcheck(t, err, "reopen without zero values") // Should succeed, no zero values.

		err = db.Insert(&bad)
		tneed(t, err, ErrZero, "inserting zero value")

		tclose(t, db)
		db, err = topen(t, "testdata/changenonzero.db", nil, User{})
		tcheck(t, err, "reopen with original type")

		bad2 := clone(bad)
		err = db.Insert(&bad2)
		tcheck(t, err, "insert user with zero value")

		tclose(t, db)
		_, err = topen(t, "testdata/changenonzero.db", nil, User2{})
		tneed(t, err, ErrZero, "reopen with invalid nonzero values")
	}

	good := User{0, "a", 1, []byte("hi"), Sub{"a"}, map[Key]Value{{1}: {[]byte("a")}}, []Elem{{1}}}
	good2 := User2{0, "a", 1, []byte("hi"), Sub2{"a"}, map[Key2]Value2{{1}: {[]byte("a")}}, []Elem2{{1}}}

	badstr := good2
	badstr.Name = ""
	testValue(good, badstr)

	badint := good2
	badint.Int = 0
	testValue(good, badint)

	badbytes := good2
	badbytes.Bytes = nil
	testValue(good, badbytes)

	badstruct := good2
	badstruct.Struct.Name = ""
	testValue(good, badstruct)

	badkey := good2
	badkey.Map = map[Key2]Value2{{0}: {[]byte("a")}}
	testValue(good, badkey)

	badvalue := good2
	badvalue.Map = map[Key2]Value2{{1}: {nil}}
	testValue(good, badvalue)

	badslice := good2
	badslice.Slice = []Elem2{{0}}
	testValue(good, badslice)
}

// When changing from ptr to nonptr, nils become zero values, and this may
// introduce a nonzero constraint violation we need to check for, such
// conversions are not allowed.
func TestChangeNonzeroPtr(t *testing.T) {
	type Struct struct {
		Nonzero int `bstore:"nonzero"`
	}
	type StructStruct struct {
		Struct Struct
	}
	type StructStructptr struct {
		Struct *Struct
	}
	type X[T any] struct {
		ID    int `bstore:"typename T"`
		Field T
	}

	path := "testdata/changenonzeroptr.db"

	tchangenonzeroptr := func(exp error, optr, n any) {
		os.Remove(path)
		db, err := topen(t, path, nil, reflect.ValueOf(optr).Elem().Interface())
		tcheck(t, err, "open")

		err = db.Insert(optr)
		tcheck(t, err, "insert")

		tclose(t, db)

		db, err = topen(t, path, nil, n)
		if exp == nil {
			tcheck(t, err, "open")
			tclose(t, db)
		} else {
			tneed(t, err, exp, "reopen where Struct Field would become a zero value")
		}
	}

	tchangenonzeroptr(ErrIncompatible, &X[*Struct]{0, nil}, X[Struct]{})
	tchangenonzeroptr(ErrIncompatible, &X[*StructStruct]{0, nil}, X[StructStruct]{})
	tchangenonzeroptr(nil, &X[*StructStructptr]{0, nil}, X[StructStructptr]{})
	tchangenonzeroptr(ErrIncompatible, &X[map[int]*Struct]{0, map[int]*Struct{0: nil}}, X[map[int]Struct]{})
	tchangenonzeroptr(nil, &X[map[int]*StructStructptr]{0, map[int]*StructStructptr{0: nil}}, X[map[int]StructStructptr]{})
	tchangenonzeroptr(ErrIncompatible, &X[[]*Struct]{0, []*Struct{nil}}, X[[]Struct]{})
	tchangenonzeroptr(nil, &X[[]*StructStructptr]{0, []*StructStructptr{nil}}, X[[]StructStructptr]{})
}

func TestNestedIndex(t *testing.T) {
	type Struct struct {
		Name string `bstore:"unique"`
	}
	type User struct {
		ID     int
		Struct Struct
	}

	type Struct2 struct {
		Name string `bstore:"index"`
	}
	type User2 struct {
		ID     int
		Struct Struct2
	}

	os.Remove("testdata/nestedindex.db")
	_, err := topen(t, "testdata/nestedindex.db", nil, User{})
	tneed(t, err, errNestedIndex, "open with nested unique tag")

	_, err = topen(t, "testdata/nestedindex.db", nil, User2{})
	tneed(t, err, errNestedIndex, "open with nested index tag")
}

func TestDrop(t *testing.T) {
	type User struct {
		ID   int
		Name string
	}
	os.Remove("testdata/drop.db")
	db, err := topen(t, "testdata/drop.db", nil)
	tcheck(t, err, "open")

	types, err := db.Types()
	tcheck(t, err, "types")
	if len(types) != 0 {
		t.Fatalf("got %v, expected 0 types", types)
	}

	tclose(t, db)
	db, err = topen(t, "testdata/drop.db", nil, User{})
	tcheck(t, err, "open")

	types, err = db.Types()
	tcheck(t, err, "types")
	if len(types) != 1 || types[0] != "User" {
		t.Fatalf("got %v, expected [User]", types)
	}

	err = db.Drop("User")
	tcheck(t, err, "drop user")

	types, err = db.Types()
	tcheck(t, err, "types")
	if len(types) != 0 {
		t.Fatalf("got %v, expected 0 types", types)
	}

	err = db.Drop("User")
	tneed(t, err, ErrAbsent, "drop absent user")
}

func TestDropReferenced(t *testing.T) {
	type User struct {
		ID      int
		Name    string
		GroupID int `bstore:"ref Group"`
	}
	type Group struct {
		ID   int
		Name string
	}

	os.Remove("testdata/dropreferenced.db")
	db, err := topen(t, "testdata/dropreferenced.db", nil, User{}, Group{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	err = db.Drop("Group")
	tneed(t, err, ErrReference, "drop referenced Group")

	err = db.Drop("User")
	tcheck(t, err, "drop User")

	err = db.Drop("Group")
	tcheck(t, err, "drop Group")

	_, err = QueryDB[User](db).List()
	tneed(t, err, ErrType, "reading removed type")
}

func TestCompatible(t *testing.T) {
	type Base[T any] struct {
		ID    int `bstore:"typename T"`
		Other T
	}

	path := "testdata/compatible.db"

	topen := func(base any, expErr error, values ...any) {
		t.Helper()

		for _, v := range values {
			os.Remove(path)

			db, err := topen(t, path, nil, base)
			tcheck(t, err, "open")
			tclose(t, db)

			db, err = topen(t, path, nil, v)
			if expErr == nil {
				tcheck(t, err, "open")
				tclose(t, db)
			} else {
				tneed(t, err, expErr, "open")
			}
		}
	}

	topen(Base[int64]{}, ErrIncompatible, Base[int]{}, Base[uint64]{})
	topen(Base[int32]{}, nil, Base[int]{}, Base[int64]{})
	topen(Base[int32]{}, ErrIncompatible, Base[uint32]{}, Base[int16]{})
	topen(Base[int16]{}, nil, Base[int]{}, Base[int64]{})
	topen(Base[int16]{}, ErrIncompatible, Base[uint16]{}, Base[int8]{})
	topen(Base[int8]{}, nil, Base[int]{}, Base[int16]{})
	topen(Base[int8]{}, ErrIncompatible, Base[uint8]{}, Base[string]{})

	topen(Base[uint64]{}, ErrIncompatible, Base[uint]{}, Base[int64]{})
	topen(Base[uint32]{}, nil, Base[uint]{}, Base[uint64]{})
	topen(Base[uint32]{}, ErrIncompatible, Base[int32]{}, Base[uint16]{})
	topen(Base[uint16]{}, nil, Base[uint]{}, Base[uint64]{})
	topen(Base[uint16]{}, ErrIncompatible, Base[int16]{}, Base[uint8]{})
	topen(Base[uint8]{}, nil, Base[uint]{}, Base[uint16]{})
	topen(Base[uint8]{}, ErrIncompatible, Base[int8]{}, Base[string]{})

	topen(Base[map[int]int16]{}, nil, Base[map[int64]int32]{})
	topen(Base[map[string]struct{}]{}, ErrIncompatible, Base[string]{}, Base[map[int]struct{}]{}, Base[map[string]string]{})
	topen(Base[[]int]{}, nil, Base[[]int32]{})
	topen(Base[[]int]{}, ErrIncompatible, Base[string]{}, Base[[]string]{})
	topen(Base[struct{ Field int }]{}, nil, Base[struct{ Field int64 }]{})
	topen(Base[struct{ Field int }]{}, ErrIncompatible, Base[string]{}, Base[struct{ Field string }]{})
}

func TestFieldRemoveAdd(t *testing.T) {
	// Records reference a type version that lists field names. If a type is updated to
	// remove a field (a new typeversion), then add a field with the same name again
	// (another new typeversion), it is a different field and we shouldn't parse the
	// data into the new field.

	type Sub struct {
		ID    int
		Email string
	}

	type Mapkey struct {
		K1 int
		K2 string
	}
	type Mapvalue struct {
		Data []byte
		Time *time.Time
	}

	type User struct {
		ID int `bstore:"typename User"`

		Byte    byte
		Int8    int8
		Int16   int16
		Int32   int32
		Int64   int64
		Uint64  uint64
		Float32 float32
		Float64 float64
		String  string
		Bytes   []byte
		Struct  Sub
		Slice   []string
		Map     map[string]struct{}
		Map2    map[Mapkey]Mapvalue
		Map3    map[Mapkey]*Mapvalue
		Time    time.Time

		Byteptr    *byte
		Int8ptr    *int8
		Int16ptr   *int16
		Int32ptr   *int32
		Int64ptr   *int64
		Uint64ptr  *uint64
		Float32ptr *float32
		Float64ptr *float64
		Stringptr  *string
		Bytesptr   *[]byte
		Structptr  *Sub
		Sliceptr   *[]string
		Mapptr     *map[string]struct{}
		Map2ptr    *map[Mapkey]Mapvalue
		Map3ptr    *map[Mapkey]*Mapvalue
		Timeptr    *time.Time
	}

	type Empty struct {
		ID int `bstore:"typename User"`
	}

	os.Remove("testdata/fieldremoveadd.db")
	db, err := topen(t, "testdata/fieldremoveadd.db", nil, User{})
	tcheck(t, err, "open")

	var u0, u1, u2 User
	now := time.Now().Round(0)
	err = db.Write(func(tx *Tx) error {
		u0 = User{
			Byte:    'a',
			Int8:    20,
			Int16:   1000,
			Int32:   1 << 20,
			Int64:   -1,
			Uint64:  ^uint64(0),
			Float32: 1.123,
			Float64: 0.000001,
			String:  "test",
			Bytes:   []byte("hi"),
			Struct:  Sub{1, "user@example.org"},
			Slice:   []string{"a", "b"},
			Time:    now,
			Map:     map[string]struct{}{"a": {}},
			Map2: map[Mapkey]Mapvalue{
				{1, "a"}: {[]byte("test"), nil},
				{1, "b"}: {nil, &now},
				{2, "a"}: {nil, nil},
				{3, "d"}: {[]byte("hi"), &now},
			},
			Map3: map[Mapkey]*Mapvalue{
				{1, "a"}: {[]byte("test"), nil},
				{1, "b"}: {nil, &now},
				{2, "a"}: {nil, nil},
				{3, "d"}: {[]byte("hi"), &now},
			},
		}
		err = tx.Insert(&u0)
		tcheck(t, err, "insert u0")

		u1 = User{
			Byteptr:    &u0.Byte,
			Int8ptr:    &u0.Int8,
			Int16ptr:   &u0.Int16,
			Int32ptr:   &u0.Int32,
			Int64ptr:   &u0.Int64,
			Uint64ptr:  &u0.Uint64,
			Float32ptr: &u0.Float32,
			Float64ptr: &u0.Float64,
			Stringptr:  &u0.String,
			Bytesptr:   &u0.Bytes,
			Structptr:  &u0.Struct,
			Sliceptr:   &u0.Slice,
			Mapptr:     &u0.Map,
			Map2ptr:    &u0.Map2,
			Map3ptr:    &u0.Map3,
			Timeptr:    &u0.Time,
		}
		err = tx.Insert(&u1)
		tcheck(t, err, "insert u1")

		u2 = User{}
		err = tx.Insert(&u2)
		tcheck(t, err, "insert u2")

		check := func(u User) {
			t.Helper()

			x := User{ID: u.ID}
			err = tx.Get(&x)
			tcheck(t, err, "get user")
			if !reflect.DeepEqual(u, x) {
				t.Fatalf("u != x: %v != %v", u, x)
			}
		}
		check(u0)
		check(u1)
		check(u2)

		return nil
	})
	tcheck(t, err, "write")

	tclose(t, db)
	db, err = topen(t, "testdata/fieldremoveadd.db", nil, Empty{}) // This masks all earlier values.
	tcheck(t, err, "open")

	err = db.Read(func(tx *Tx) error {
		check := func(u User) {
			t.Helper()

			e := Empty{ID: u.ID}
			x := Empty{ID: u.ID}
			err = tx.Get(&x)
			tcheck(t, err, "get user")
			if !reflect.DeepEqual(e, x) {
				t.Fatalf("e != x: %v != %v", e, x)
			}
		}
		check(u0)
		check(u1)
		check(u2)
		return nil
	})
	tcheck(t, err, "read")

	tclose(t, db)
	db, err = topen(t, "testdata/fieldremoveadd.db", nil, User{}) // The fields are back, but they are masked for old values.
	tcheck(t, err, "open")

	err = db.Read(func(tx *Tx) error {
		check := func(u User) {
			t.Helper()

			e := User{ID: u.ID}
			x := User{ID: u.ID}
			err = tx.Get(&x)
			tcheck(t, err, "get user")
			if !reflect.DeepEqual(e, x) {
				t.Fatalf("e != x: %v != %v", e, x)
			}
		}
		check(u0)
		check(u1)
		check(u2)
		return nil
	})
	tcheck(t, err, "read")

	tclose(t, db)
}

func TestAddNonzero(t *testing.T) {
	// We can only add a new nonzero field if there are no records yet.
	type User struct {
		ID int
	}
	type User2 struct {
		ID   int    `bstore:"typename User"`
		Name string `bstore:"nonzero"`
	}

	os.Remove("testdata/addnonzero.db")
	db, err := topen(t, "testdata/addnonzero.db", nil, User{})
	tcheck(t, err, "open")

	tclose(t, db)
	db, err = topen(t, "testdata/addnonzero.db", nil, User2{}) // No records yet, all good.
	tcheck(t, err, "open")

	tclose(t, db)
	os.Remove("testdata/addnonzero.db")
	db, err = topen(t, "testdata/addnonzero.db", nil, User{})
	tcheck(t, err, "open")

	err = db.Insert(&User{})
	tcheck(t, err, "insert user")

	tclose(t, db)
	_, err = topen(t, "testdata/addnonzero.db", nil, User2{})
	tneed(t, err, ErrZero, "adding nonzero field with records present")
}

func TestDupField(t *testing.T) {
	type User struct {
		ID   int
		Name string `bstore:"name ID"`
	}

	os.Remove("testdata/dupfield.db")
	_, err := topen(t, "testdata/dupfield.db", nil, User{})
	tneed(t, err, ErrType, "open type with duplicate field name")
}

func TestTransaction(t *testing.T) {
	type User struct {
		ID    int
		Field string
	}

	os.Remove("testdata/transaction.db")
	db, err := topen(t, "testdata/transaction.db", nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	tx, err := db.Begin(true)
	tcheck(t, err, "begin")

	u := User{}
	err = tx.Insert(&u)
	tcheck(t, err, "insert")

	err = tx.Get(&u)
	tcheck(t, err, "get")

	err = tx.Commit()
	tcheck(t, err, "commit")

	err = tx.Commit()
	tneed(t, err, errTxClosed, "commit on closed tx")

	err = tx.Get(&User{})
	tneed(t, err, errTxClosed, "get on closed tx")

	err = tx.Insert(&User{})
	tneed(t, err, errTxClosed, "insert on closed tx")

	err = tx.Update(&User{})
	tneed(t, err, errTxClosed, "update on closed tx")

	err = tx.Delete(&User{})
	tneed(t, err, errTxClosed, "delete on closed tx")

	err = (&Tx{}).Get(&User{})
	tneed(t, err, errTxClosed, "delete on closed tx")

	err = tx.Rollback()
	tneed(t, err, errTxClosed, "rollback on closed tx")

	tx, err = db.Begin(false)
	tcheck(t, err, "begin")

	err = tx.Get(&u)
	tcheck(t, err, "get")

	u.Field = "changed"
	err = tx.Update(&u)
	if err == nil {
		t.Fatalf("did not get error for write on read-only transaction")
	}

	err = tx.Commit()
	tneed(t, err, bolt.ErrTxNotWritable, "commit a read-only tx")

	tx, err = db.Begin(false)
	tcheck(t, err, "begin")
	err = tx.Rollback()
	tcheck(t, err, "rollback")
}

func TestWriteto(t *testing.T) {
	type User struct {
		ID int
	}

	os.Remove("testdata/writeto.db")
	db, err := topen(t, "testdata/writeto.db", nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	u := User{}
	err = db.Insert(&u)
	tcheck(t, err, "insert")

	err = db.Read(func(tx *Tx) error {
		f, err := os.Create("testdata/writeto2.db")
		tcheck(t, err, "create")
		defer os.Remove(f.Name())
		defer f.Close()
		_, err = tx.WriteTo(f)
		tcheck(t, err, "writeto")
		err = f.Sync()
		tcheck(t, err, "sync")

		ndb, err := topen(t, "testdata/writeto2.db", nil, User{})
		tcheck(t, err, "open")
		defer tclose(t, ndb)
		nu := User{u.ID}
		err = ndb.Get(&nu)
		tcheck(t, err, "get")

		return nil
	})
	tcheck(t, err, "read")
}

type Custom struct {
	Int     int32
	private int
}

func (c Custom) MarshalBinary() (data []byte, err error) {
	return []byte(fmt.Sprintf("%d", c.Int)), nil
}

func (c *Custom) UnmarshalBinary(data []byte) error {
	i, err := strconv.ParseInt(string(data), 10, 32)
	if err != nil {
		return err
	}
	*c = Custom{Int: int32(i), private: 1}
	return nil
}

func TestBinarymarshal(t *testing.T) {
	type BadPK struct {
		Custom Custom
	}

	type BadIndex struct {
		ID     int
		Custom Custom `bstore:"index"`
	}

	type User struct {
		ID     int
		Custom Custom // Stored with MarshalBinary
	}

	os.Remove("testdata/binarymarshal.db")
	_, err := topen(t, "testdata/binarymarshal.db", nil, BadPK{})
	tneed(t, err, ErrType, "bad binarymarshal for pk")

	_, err = topen(t, "testdata/binarymarshal.db", nil, BadIndex{})
	tneed(t, err, ErrType, "bad binarymarshal for index")

	db, err := topen(t, "testdata/binarymarshal.db", nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	u := User{0, Custom{Int: 123}}
	err = db.Insert(&u)
	tcheck(t, err, "insert")

	err = db.Get(&u)
	tcheck(t, err, "get")

	u.Custom.Int += 1
	err = db.Update(&u)
	tcheck(t, err, "update")

	users, err := QueryDB[User](db).List()
	tcompare(t, err, users, []User{u}, "query list")

	n, err := QueryDB[User](db).FilterEqual("Custom", u.Custom).Count()
	tcompare(t, err, n, 1, "filterequal count")

	err = QueryDB[User](db).FilterGreater("Custom", u.Custom).Err()
	tneed(t, err, ErrParam, "bad filter compare on binarymarshal")
}

// Test seamlessly changing between pointer fields.
func TestChangePtr(t *testing.T) {
	type User struct {
		ID      int
		Age     int
		Name    string
		Created time.Time
		BM      bm
	}
	type User2 struct {
		ID      int `bstore:"typename User"`
		Age     *int
		Name    *string
		Created *time.Time
		BM      *bm
	}

	path := "testdata/changeptr.db"
	os.Remove(path)
	db, err := topen(t, path, nil, User{})
	tcheck(t, err, "open")

	u0 := User{0, 10, "test", time.Now().Round(0), bm{"test"}}
	u1 := User{}

	err = db.Insert(&u0, &u1)
	tcheck(t, err, "insert")

	err = db.Get(&u0, &u1)
	tcheck(t, err, "get")

	tclose(t, db)

	db, err = topen(t, path, nil, User2{})
	tcheck(t, err, "open")

	x0 := User2{ID: u0.ID}
	x1 := User2{ID: u1.ID}
	err = db.Get(&x0)
	tcheck(t, err, "get")
	err = db.Get(&x1)
	tcheck(t, err, "get")

	if x0.Age == nil || x0.Name == nil || x0.Created == nil || x0.BM == nil {
		t.Fatalf("unexpected nil values in x0 %v vs u0 %v", x0, u0)
	}
	if *x0.Age != u0.Age || *x0.Name != u0.Name || !x0.Created.Equal(u0.Created) || *x0.BM != u0.BM {
		t.Fatalf("unexpected values in x0 %v vs u0 %v", x0, u0)
	}

	if x1.Age != nil || x1.Name != nil || x1.Created != nil || x1.BM != nil {
		t.Fatalf("unexpected non-nil values in x1 %v vs u1 %v", x1, u1)
	}

	tclose(t, db)

	db, err = topen(t, path, nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	err = db.Get(&u0, &u1)
	tcheck(t, err, "get")

	if u0.Age != *x0.Age || u0.Name != *x0.Name || !u0.Created.Equal(*x0.Created) || u0.BM != *x0.BM {
		t.Fatalf("unexpected values in u0 %v vs x0 %v", u0, x0)
	}
	var zerotime time.Time
	var zerobm bm
	if u1.Age != 0 || u1.Name != "" || u1.Created != zerotime || u1.BM != zerobm {
		t.Fatalf("unexpected nonzero values in u1 %v vs x1 %v", u1, x1)
	}
}

func TestHintAppend(t *testing.T) {
	type User struct {
		ID   int
		Name string
	}

	path := "testdata/hintappend.db"
	os.Remove(path)
	db, err := topen(t, path, nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	err = db.HintAppend(true, User{})
	tcheck(t, err, "hintappend")

	err = db.HintAppend(true, struct{}{})
	tneed(t, err, ErrType, "bad type")

	u0 := User{}
	err = db.Insert(&u0)
	tcheck(t, err, "insert after hintappend")

	x0 := User{ID: u0.ID}
	err = db.Get(&x0)
	tcompare(t, err, x0, u0, "get")

	err = db.HintAppend(false, User{})
	tcheck(t, err, "hintappend false")

	u1 := User{}
	err = db.Insert(&u1)
	tcheck(t, err, "insert after hintappend false")

	err = db.Get(&x0)
	tcompare(t, err, x0, u0, "get")

	x1 := User{ID: u1.ID}
	err = db.Get(&x1)
	tcheck(t, err, "get")
	tcompare(t, err, x1, u1, "get")
}

// Test that registering a type that is referenced returns an error if the
// referencing type isn't registered at the same time. If we would allow it, a
// user can remove keys from the referenced type that are still referenced by
// the referencing type, resulting in inconsistency.
func TestRegisterRef(t *testing.T) {
	type User struct {
		ID      int
		GroupID int `bstore:"nonzero,ref Group"`
	}
	type Group struct {
		ID int
	}
	type User2 struct {
		ID      int `bstore:"typename User"`
		GroupID int // no ref
	}
	type Group2 struct {
		ID   int `bstore:"typename Group"`
		Name string
	}

	path := "testdata/registerref.db"
	os.Remove(path)
	db, err := topen(t, path, nil, User{}, Group{})
	tcheck(t, err, "open")

	var g0, g1 Group
	err = db.Insert(&g0, &g1)
	tcheck(t, err, "insert")

	u0 := User{GroupID: g0.ID}
	u1 := User{GroupID: g0.ID}
	u2 := User{GroupID: g1.ID}
	err = db.Insert(&u0, &u1, &u2)
	tcheck(t, err, "insert user")

	tclose(t, db)

	_, err = topen(t, path, nil, Group{})
	tneed(t, err, ErrType, "open group without user")

	db, err = topen(t, path, nil, User2{}, Group{})
	tcheck(t, err, "open to remove reference from group and create new typeversion for group")
	tclose(t, db)

	db, err = topen(t, path, nil, Group{})
	tcheck(t, err, "can now open only group, no more reference")
	tclose(t, db)

	// Reset.
	db, err = topen(t, path, nil, User{}, Group{})
	tcheck(t, err, "base case")
	tclose(t, db)

	// Now remove ref while at same time updating the referenced type.
	db, err = topen(t, path, nil, User2{}, Group2{})
	tcheck(t, err, "open to removing ref on already changed group type")
	tclose(t, db)
}

func TestRefUpdateIndex(t *testing.T) {
	type Mailbox struct {
		ID int64
	}

	type Message0 struct {
		ID        int64 `bstore:"typename Message"`
		MailboxID int64 `bstore:"ref Mailbox"`
		MessageID string
	}

	type Message struct {
		ID        int64
		MailboxID int64  `bstore:"ref Mailbox"`
		MessageID string `bstore:"index"`
	}

	path := "testdata/refupdateindex.db"
	os.Remove(path)

	db, err := topen(t, path, nil, Message0{}, Mailbox{})
	tcheck(t, err, "open")
	tclose(t, db)

	db, err = topen(t, path, nil, Message{}, Mailbox{})
	tcheck(t, err, "open with message that introduces field")
	tclose(t, db)
}

func TestChangeType(t *testing.T) {
	type T0 struct {
		ID   int64 `bstore:"typename T"`
		Name string
		S    string
	}

	type T1 struct {
		ID   int64    `bstore:"typename T"`
		Name []string `bstore:"name Name2"`
		S    string
	}

	type T2 struct {
		ID   int64 `bstore:"typename T"`
		Name string
		S    string
	}

	path := "testdata/changetype.db"
	os.Remove(path)

	db, err := topen(t, path, nil, T0{})
	tcheck(t, err, "open")
	v0 := T0{Name: "test", S: "s"}
	err = db.Insert(&v0)
	tcheck(t, err, "insert")
	tclose(t, db)

	db, err = topen(t, path, nil, T1{})
	tcheck(t, err, "open with renamed field of different type")
	v1 := T1{ID: v0.ID, S: "s"}
	err = db.Get(&v1)
	tcompare(t, err, v1, T1{v0.ID, nil, "s"}, "get")
	tclose(t, db)

	db, err = topen(t, path, nil, T2{})
	tcheck(t, err, "open with renamed field of different type")
	v2 := T2{ID: v0.ID, S: "s"}
	err = db.Get(&v2)
	tcompare(t, err, v2, T2{v0.ID, "", "s"}, "get")
	tclose(t, db)
}

// Test that list and map types get their fields propagated in newer type
// versions.
func TestChangeTypeListMap(t *testing.T) {
	type Key struct {
		Name string
	}
	type Value struct {
		Value int
	}
	type List2 struct {
		Map map[Key]Value
	}
	type Sub struct {
		Elems []List2
	}
	type List struct {
		Sub Sub
	}
	type T0 struct {
		ID    int64 `bstore:"typename T"`
		A     string
		List  []List
		List2 []List2
		Map   map[Key]map[Key]Value
	}

	type T1 struct {
		ID    int64  `bstore:"typename T"`
		B     string // Changed.
		List  []List
		List2 []List2
		Map   map[Key]map[Key]Value
	}

	path := "testdata/changetypelistmap.db"
	os.Remove(path)

	db, err := topen(t, path, nil, T0{})
	tcheck(t, err, "open")
	v0 := T0{
		A: "test",
		List: []List{
			{
				Sub{
					Elems: []List2{
						{
							Map: map[Key]Value{
								{"x"}: {1},
							},
						},
					},
				},
			},
		},
		List2: []List2{
			{
				Map: map[Key]Value{
					{"y"}: {2},
				},
			},
		},
		Map: map[Key]map[Key]Value{
			{"y"}: {
				{"a"}: {3},
			},
		},
	}
	err = db.Insert(&v0)
	tcheck(t, err, "insert")
	tclose(t, db)

	db, err = topen(t, path, nil, T1{})
	tcheck(t, err, "open with renamed field of different type")
	v1 := T1{ID: v0.ID}
	err = db.Get(&v1)
	tcompare(t, err, v1.List, v0.List, "get")
	tcompare(t, err, v1.List2, v0.List2, "get")
	tcompare(t, err, v1.Map, v0.Map, "get")
	tclose(t, db)
}

func TestChangeTypeSub(t *testing.T) {
	type Sub1 struct {
		Name string
	}
	type Sub2 struct {
		Name []string `bstore:"name Name2"`
	}
	type T0 struct {
		ID  int64 `bstore:"typename T"`
		Sub Sub1
	}

	type T1 struct {
		ID  int64 `bstore:"typename T"`
		Sub Sub2
	}

	type T2 struct {
		ID  int64 `bstore:"typename T"`
		Sub Sub1
	}

	path := "testdata/changetypesub.db"
	os.Remove(path)

	db, err := topen(t, path, nil, T0{})
	tcheck(t, err, "open")
	v0 := T0{0, Sub1{"test"}}
	err = db.Insert(&v0)
	tcheck(t, err, "insert")
	tclose(t, db)

	db, err = topen(t, path, nil, T1{})
	tcheck(t, err, "open with renamed field of different type")
	v1 := T1{v0.ID, Sub2{[]string{"x"}}}
	err = db.Get(&v1)
	tcompare(t, err, v1, T1{v0.ID, Sub2{nil}}, "get")
	tclose(t, db)

	db, err = topen(t, path, nil, T2{})
	tcheck(t, err, "open with renamed field of different type")
	v2 := T2{v0.ID, Sub1{"x"}}
	err = db.Get(&v2)
	tcompare(t, err, v2, T2{v0.ID, Sub1{""}}, "get")
	tclose(t, db)
}

func bcheck(b *testing.B, err error, msg string) {
	if err != nil {
		b.Fatalf("%s: %s", msg, err)
	}
}

func BenchmarkGet(b *testing.B) {
	type User struct {
		ID   int
		Name string `bstore:"unique"`
	}
	path := "testdata/benchmarkget.db"
	os.Remove(path)
	db, err := Open(path, nil, User{})
	bcheck(b, err, "open")

	const count = 100 * 1000
	err = db.Write(func(tx *Tx) error {
		for i := 0; i < count; i++ {
			u := User{Name: fmt.Sprintf("user%d", i)}
			err := tx.Insert(&u)
			bcheck(b, err, "insert")
		}
		return nil
	})
	bcheck(b, err, "write")

	rnd := mathrand.New(mathrand.NewSource(1))
	b.ResetTimer()

	err = db.Read(func(tx *Tx) error {
		for i := 0; i < b.N; i++ {
			c := rnd.Int63n(count)
			name := fmt.Sprintf("user%d", c)
			_, err := QueryTx[User](tx).FilterEqual("Name", name).Get()
			bcheck(b, err, "get")
		}
		return nil
	})
	bcheck(b, err, "read")
}

func BenchmarkRange(b *testing.B) {
	type User struct {
		ID   int
		Name string `bstore:"unique"`
	}
	path := "testdata/benchmarkrange.db"
	os.Remove(path)
	db, err := Open(path, nil, User{})
	bcheck(b, err, "open")

	const count = 100 * 1000
	err = db.Write(func(tx *Tx) error {
		for i := 0; i < count; i++ {
			u := User{Name: fmt.Sprintf("user%07d", i)}
			err := tx.Insert(&u)
			bcheck(b, err, "insert")
		}
		return nil
	})
	bcheck(b, err, "write")

	rnd := mathrand.New(mathrand.NewSource(1))
	b.ResetTimer()

	err = db.Read(func(tx *Tx) error {
		for i := 0; i < b.N; i++ {
			c := rnd.Int63n(count)
			name := fmt.Sprintf("user%07d", c)
			if c < count/2 {
				_, err = QueryTx[User](tx).FilterGreater("Name", name).Limit(1000).SortAsc("Name").List()
			} else {
				_, err = QueryTx[User](tx).FilterLess("Name", name).Limit(1000).SortDesc("Name").List()
			}
			bcheck(b, err, "list")
		}
		return nil
	})
	bcheck(b, err, "read")
}

func BenchmarkInsert(b *testing.B) {
	type User struct {
		ID   int
		Name string
	}
	path := "testdata/benchmarkinsert.db"
	os.Remove(path)
	db, err := Open(path, nil, User{})
	bcheck(b, err, "open")

	b.ResetTimer()

	err = db.Write(func(tx *Tx) error {
		for i := 0; i < b.N; i++ {
			u := User{Name: fmt.Sprintf("user%d", i)}
			err := tx.Insert(&u)
			bcheck(b, err, "insert")
		}
		return nil
	})
	bcheck(b, err, "write")
}

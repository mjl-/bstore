package bstore

import (
	"io"
	"math"
	"os"
	"testing"
	"time"
)

// Test loading data written with different ondiskVersions.
// We have this same data structure written with v1 and with v2 (library version).
// We check that we can read the data properly with the latest code.
func TestCompatv2(t *testing.T) {
	type Other struct {
		O string
	}
	type OtherOther struct {
		Other
	}
	type X struct {
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
		Slice3  []Other
		Map     map[string]int
		Map2    map[string]struct{ Name string }
		Map3    map[Other]int
		Map4    map[int]Other
		Float32 float32
		Float64 float64
		BM      bm
		Named   Other
		Other
		More OtherOther
	}

	tm, err := time.Parse(time.RFC3339, "2022-10-18T07:05:02-02:00")
	tcheck(t, err, "parse time")

	xtrue := true

	expect := []X{
		{
			ID: 1,
		},
		{
			ID:      2,
			String:  "x",
			Time:    tm,
			Bool:    true,
			Boolptr: &xtrue,
			Uint:    ^uint64(0),
			Bytes:   []byte("bytes"),
			Struct:  struct{ Value int }{123},
			Slice:   []string{"a", "b", "c"},
			Slice2:  []struct{ Name string }{{"X"}, {"Y"}},
			Slice3:  []Other{{"Slice3value"}, {""}},
			Map:     map[string]int{"map": 1 << 30},
			Map2:    map[string]struct{ Name string }{"map2": {"Map2 name"}},
			Map3:    map[Other]int{{"Map3key"}: -1, {""}: 0, {"Otherkey"}: 0},
			Map4:    map[int]Other{0: {}, -1: {}, 1: {"map4value"}},
			Float32: -1.23e20,
			Float64: math.Inf(-1),
			BM:      bm{"bmvalue"},
			Named:   Other{"other"},
			Other:   Other{"other2"},
			More:    OtherOther{Other: Other{"moreother"}},
		},
	}

	// Generate a database file with environment variable set. Can be used
	// on older code (that has v1 only), but needs ctxbg parameters removed
	// since that parameter didn't exist then.
	//
	// bstore_compat_write=testdata/compatv2.db go test -run '^TestCompatv2$'
	if path := os.Getenv("bstore_compat_write"); path != "" {
		db, err := topen(t, path, nil, X{})
		tcheck(t, err, "open")

		err = db.Write(ctxbg, func(tx *Tx) error {
			for _, x := range expect {
				err := tx.Insert(&x)
				tcheck(t, err, "insert")
			}
			return nil
		})
		tcheck(t, err, "write")

		tclose(t, db)
	}

	// Copy the files, they are changed when opened, and w don't want to
	// constantly have modified files according to git.
	copyFile := func(src, dst string) {
		t.Helper()
		sf, err := os.Open(src)
		tcheck(t, err, "open source")
		defer sf.Close()
		df, err := os.Create(dst)
		tcheck(t, err, "create dest")
		_, err = io.Copy(df, sf)
		tcheck(t, err, "copy")
		err = df.Close()
		tcheck(t, err, "close")
	}
	copyFile("testdata/compatv1.db", "testdata/tmp.compatv1.db")
	copyFile("testdata/compatv2.db", "testdata/tmp.compatv2.db")

	// Read a v1 database with this type.
	db1, err := topen(t, "testdata/tmp.compatv1.db", nil, X{})
	tcheck(t, err, "open")
	records1, err := QueryDB[X](ctxbg, db1).List()
	tcompare(t, err, records1, expect, "list records with v1")
	tclose(t, db1)

	// Read a v2 database with this type.
	os.Setenv("bstore_schema_check", "unchanged")
	defer os.Setenv("bstore_schema_check", "")
	db2, err := topen(t, "testdata/tmp.compatv2.db", nil, X{})
	tcheck(t, err, "open")
	records2, err := QueryDB[X](ctxbg, db2).List()
	tcompare(t, err, records2, expect, "list records for v2")
	tclose(t, db2)
}

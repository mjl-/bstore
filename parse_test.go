package bstore

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func FuzzParse(f *testing.F) {
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
		Name       string
		Registered time.Time

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
		Coords  [2]float64
		BM      bm

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
		CoordsPtr  *[2]float64
		BMptr      *bm
	}
	t := reflect.TypeOf(User{})
	tv, err := gatherTypeVersion(t)
	if err != nil {
		f.Fatalf("type version: %v", err)
	}

	st := storeType{
		Type:     t,
		Current:  tv,
		Versions: map[uint32]*typeVersion{tv.Version: tv},
	}
	f.Fuzz(func(t *testing.T, key, value []byte) {
		v := reflect.New(st.Type).Elem()
		err := st.parse(v, value)
		if err != nil && !errors.Is(err, ErrStore) && !errors.Is(err, ErrZero) {
			t.Errorf("unexpected error %v", err)
		}

		_, err = parseMap(st.Versions, key, value)
		if err != nil && !errors.Is(err, ErrStore) && !errors.Is(err, ErrZero) {
			t.Errorf("unexpected error %v", err)
		}
	})
}

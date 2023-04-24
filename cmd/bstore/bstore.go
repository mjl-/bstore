package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"sort"
	"unicode/utf8"

	bolt "go.etcd.io/bbolt"

	"github.com/mjl-/bstore"
)

func xcheckf(err error, format string, args ...any) {
	if err != nil {
		msg := fmt.Sprintf(format, args...)
		log.Fatalf("%s: %s", msg, err)
	}
}

func usage() {
	log.Println("usage: bstore types file.db")
	log.Println("       bstore drop file.db type")
	log.Println("       bstore dumptype file.db type")
	log.Println("       bstore keys file.db type")
	log.Println("       bstore records file.db type")
	log.Println("       bstore record file.db type key")
	log.Println("       bstore exportcsv file.db type >export.csv")
	log.Println("       bstore exportjson [flags] file.db [type] >export.json")
	log.Println("       bstore dumpall file.db")
	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
	}
	cmd, args := args[0], args[1:]
	switch cmd {
	default:
		usage()
	case "types":
		types(args)
	case "drop":
		drop(args)
	case "dumptype":
		dumptype(args)
	case "keys":
		keys(args)
	case "records":
		records(args)
	case "record":
		record(args)
	case "exportcsv":
		exportcsv(args)
	case "exportjson":
		exportjson(args)
	case "dumpall":
		dumpall(args)
	}
}

func xopen(path string) *bstore.DB {
	_, err := os.Stat(path)
	xcheckf(err, "stat")
	db, err := bstore.Open(path, nil)
	xcheckf(err, "open database")
	return db
}

func types(args []string) {
	if len(args) != 1 {
		usage()
	}
	db := xopen(args[0])
	l, err := db.Types()
	xcheckf(err, "list types")
	for _, name := range l {
		fmt.Println(name)
	}
}

func drop(args []string) {
	if len(args) != 2 {
		usage()
	}
	db := xopen(args[0])
	err := db.Drop(args[1])
	xcheckf(err, "drop type")
}

func dumptype(args []string) {
	if len(args) != 2 {
		usage()
	}
	db, err := bolt.Open(args[0], 0600, nil)
	xcheckf(err, "bolt open")
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(args[1]))
		if b == nil {
			return fmt.Errorf("no bucket for type")
		}

		tb := b.Bucket([]byte("types"))
		if tb == nil {
			return fmt.Errorf("missing types bucket for type")
		}

		var latest map[string]any
		var latestVersion int
		err = tb.ForEach(func(bk, bv []byte) error {
			var t map[string]any
			err := json.Unmarshal(bv, &t)
			xcheckf(err, "unmarshal type")
			v, ok := t["Version"]
			if !ok {
				return fmt.Errorf("missing field Version in type")
			}
			vv, ok := v.(float64)
			if !ok {
				return fmt.Errorf("field Version in type is %v, should be float64", v)
			}
			xv := int(vv)
			if latest == nil || xv > latestVersion {
				latest = t
				latestVersion = xv
			}
			return nil
		})
		xcheckf(err, "type bucket foreach")

		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "\t")
		err = enc.Encode(latest)
		xcheckf(err, "marshal type")
		return nil
	})
	xcheckf(err, "view tx")
}

func keys(args []string) {
	if len(args) != 2 {
		usage()
	}

	db := xopen(args[0])
	err := db.Keys(args[1], func(v any) error {
		fmt.Println(v)
		return nil
	})
	xcheckf(err, "keys")
}

func records(args []string) {
	if len(args) != 2 {
		usage()
	}

	db := xopen(args[0])
	var fields []string
	err := db.Records(args[1], &fields, func(v map[string]any) error {
		return json.NewEncoder(os.Stdout).Encode(v)
	})
	xcheckf(err, "records")
}

func record(args []string) {
	if len(args) != 3 {
		usage()
	}

	db := xopen(args[0])
	var fields []string
	record, err := db.Record(args[1], args[2], &fields)
	xcheckf(err, "record")
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "\t")
	err = enc.Encode(record)
	xcheckf(err, "marshal record")
}

func exportcsv(args []string) {
	if len(args) != 2 {
		usage()
	}

	db := xopen(args[0])
	var w *csv.Writer
	var fields []string
	var record []string
	err := db.Records(args[1], &fields, func(v map[string]any) error {
		if w == nil {
			w = csv.NewWriter(os.Stdout)
			if err := w.Write(fields); err != nil {
				return err
			}
			record = make([]string, len(fields))
		}
		for i, f := range fields {
			var s string
			record[i] = ""
			switch fv := v[f].(type) {
			case []byte:
				if len(fv) == 0 {
					continue
				}
				if utf8.Valid(fv) {
					s = fmt.Sprintf("%q", fv)
				} else {
					s = base64.StdEncoding.EncodeToString(fv)
				}
			case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr, float32, float64, complex64, complex128, string:
				s = fmt.Sprintf("%v", v[f])
			default:
				rv := reflect.ValueOf(v[f])
				switch rv.Kind() {
				case reflect.Slice, reflect.Map:
					if rv.Len() == 0 {
						continue
					}
				case reflect.Ptr:
					if rv.IsNil() {
						continue
					}
				}
				buf, err := json.Marshal(v[f])
				if err != nil {
					s = fmt.Sprintf("%v", v[f])
				} else {
					s = string(buf)
				}
			}
			record[i] = s
		}
		return w.Write(record)
	})
	xcheckf(err, "records")
	if w != nil {
		w.Flush()
		err = w.Error()
		xcheckf(err, "write csv")
	}
}

func exportjson(args []string) {
	if len(args) == 0 || len(args) > 2 {
		usage()
	}

	db := xopen(args[0])
	if len(args) == 1 {
		err := exportAllJSON(db)
		xcheckf(err, "export all")
	} else {
		err := exportJSON(db, args[1])
		xcheckf(err, "export")
	}
}

type exportErr struct{ err error }

type exporter struct {
	db     *bstore.DB
	indent int // additional indenting
	bw     *bufio.Writer
}

func (e *exporter) check(err error, msg string) {
	if err != nil {
		panic(exportErr{fmt.Errorf("%s: %w", msg, err)})
	}
}

func (e *exporter) writePrefix(count int) {
	for n := e.indent + count; n > 0; n-- {
		e.writeStr("\t")
	}
}

func (e *exporter) writeStr(s string) {
	if _, err := e.bw.WriteString(s); err != nil {
		panic(exportErr{err})
	}
}

func (e *exporter) write(buf []byte) {
	if _, err := e.bw.Write(buf); err != nil {
		panic(exportErr{err})
	}
}

func (e *exporter) recover(rerr *error) {
	x := recover()
	if x == nil {
		return
	}
	err, ok := x.(exportErr)
	if ok {
		*rerr = err.err
		return
	}
	panic(x)
}

func (e *exporter) export(name string) error {
	e.writeStr("[")

	var fields []string
	var n int
	err := e.db.Records(name, &fields, func(record map[string]any) error {
		if n > 0 {
			e.writeStr(",")
		}
		e.writeStr("\n")
		e.writePrefix(1)
		buf, err := json.Marshal(record)
		e.check(err, "marshal record")
		e.write(buf)
		n++
		return nil
	})
	if err != nil {
		return err
	}
	e.writeStr("\n")
	e.writePrefix(0)
	e.writeStr("]")
	return nil
}

func exportJSON(db *bstore.DB, typeName string) (rerr error) {
	e := exporter{db, 0, bufio.NewWriter(os.Stdout)}
	defer e.recover(&rerr)
	err := e.export(typeName)
	if err != nil {
		return err
	}
	e.writeStr("\n")
	return e.bw.Flush()
}

func exportAllJSON(db *bstore.DB) (rerr error) {
	e := exporter{db, 1, bufio.NewWriter(os.Stdout)}
	defer e.recover(&rerr)

	types, err := db.Types()
	if err != nil {
		return err
	}
	sort.Strings(types)
	e.writeStr("{")
	for i, t := range types {
		if i > 0 {
			e.writeStr(",")
		}
		e.writeStr("\n\t")
		buf, err := json.Marshal(t)
		e.check(err, "marshal type name")
		e.write(buf)
		e.writeStr(": ")
		if err := e.export(t); err != nil {
			return err
		}
	}
	e.writeStr("\n}\n")
	return e.bw.Flush()
}

func dumpall(args []string) {
	if len(args) != 1 {
		usage()
	}

	db, err := bolt.Open(args[0], 0600, nil)
	xcheckf(err, "bolt open")
	err = db.View(func(tx *bolt.Tx) error {
		err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			fmt.Println("#", string(name))
			var indices []string
			err := b.ForEach(func(bk, bv []byte) error {
				if bytes.HasPrefix(bk, []byte("index.")) {
					indices = append(indices, string(bk))
				} else {
					switch string(bk) {
					case "records", "types":
					default:
						log.Printf("unrecognized subbucket %q for type %q", bk, name)
					}
				}
				return nil
			})
			xcheckf(err, "foreach subbucket")

			sort.Slice(indices, func(i, j int) bool {
				return indices[i] < indices[j]
			})

			tb := b.Bucket([]byte("types"))
			if tb == nil {
				log.Printf("missing types bucket for %q", name)
			} else {
				fmt.Println("## types")
				err = tb.ForEach(func(bk, bv []byte) error {
					fmt.Printf("\t%s\n", bv)
					return nil
				})
				xcheckf(err, "types foreach")
				fmt.Println()
			}

			for _, idx := range indices {
				ib := b.Bucket([]byte(idx))
				if ib == nil {
					log.Printf("missing index bucket for type %q index %q", name, idx)
					continue
				}
				fmt.Printf("## %s\n", idx)
				err = ib.ForEach(func(bk, bv []byte) error {
					fmt.Printf("\t%x\n", bk)
					return nil
				})
				xcheckf(err, "foreach")
				fmt.Println()
			}

			rb := b.Bucket([]byte("records"))
			if rb == nil {
				log.Printf("missing records bucket for type %q", name)
				return nil
			}
			fmt.Println("## records")
			err = rb.ForEach(func(bk, bv []byte) error {
				fmt.Printf("\t%x %x\n", bk, bv)
				return nil
			})
			xcheckf(err, "foreach")
			return nil
		})
		xcheckf(err, "foreach bucket")
		return nil
	})
	xcheckf(err, "view")
}

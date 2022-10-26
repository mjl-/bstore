package bstore

import (
	"errors"
	"math"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestQuery(t *testing.T) {
	type User struct {
		ID   int
		Name string
		Num  int
	}

	os.Remove("testdata/query.db")
	db, err := topen(t, "testdata/query.db", nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	u0 := User{Name: "mjl"}
	err = db.Insert(&u0)
	tcheck(t, err, "insert u0")

	u1 := User{Name: "gopher"}
	err = db.Insert(&u1)
	tcheck(t, err, "insert u1")

	err = QueryDB[*User](db).Err()
	tneed(t, err, ErrType, "pointer type")

	q := func() *Query[User] {
		return QueryDB[User](db)
	}

	x0, err := q().FilterID(u0.ID).Get()
	tcheck(t, err, "get x0")
	if !reflect.DeepEqual(u0, x0) {
		t.Fatalf("u0 != x0: %v != %v", u0, x0)
	}

	x0, err = q().FilterID(u0.ID).SortAsc("ID").Get()
	tcheck(t, err, "get x0")
	if !reflect.DeepEqual(u0, x0) {
		t.Fatalf("u0 != x0: %v != %v", u0, x0)
	}

	_, err = q().FilterID(u0.ID + 999).Get()
	tneed(t, err, ErrAbsent, "get for nonexistent record")

	_, err = q().Get()
	tneed(t, err, ErrMultiple, "get with multiple selected records")

	x0, err = q().FilterNonzero(User{ID: u0.ID}).Get()
	tcheck(t, err, "get x0")
	if !reflect.DeepEqual(u0, x0) {
		t.Fatalf("u0 != x0: %v != %v", u0, x0)
	}

	lexp := []User{u0, u1}
	l, err := q().List()
	tcompare(t, err, l, lexp, "list")

	l, err = q().FilterNotEqual("ID", u0.ID, u1.ID).List()
	tcompare(t, err, l, []User{}, "empty selection returns empty list, not nil")

	x0, err = q().FilterEqual("ID", u0.ID).Get()
	tcheck(t, err, "get x0")
	if !reflect.DeepEqual(u0, x0) {
		t.Fatalf("u0 != x0: %v != %v", u0, x0)
	}

	x1, err := q().FilterNotEqual("ID", u0.ID).Get()
	tcheck(t, err, "get not u0")
	if !reflect.DeepEqual(u1, x1) {
		t.Fatalf("u1 != x1: %v != %v", u1, x1)
	}

	n, err := q().FilterEqual("ID", u0.ID, u1.ID).Count()
	tcompare(t, err, n, 2, "count multiple")

	err = q().FilterEqual("ID").Err()
	tneed(t, err, ErrParam, "equal without values")

	err = q().FilterEqual("Bogus", "").Err()
	tneed(t, err, ErrParam, "unknown field")

	n, err = q().FilterNotEqual("ID", u0.ID, u1.ID).Count()
	tcompare(t, err, n, 0, "count not multiple")

	compareID := func(id int, exp User) {
		t.Helper()
		x := User{ID: id}
		err := db.Get(&x)
		tcheck(t, err, "get user")
		if !reflect.DeepEqual(x, exp) {
			t.Fatalf("compare, got %v, expect %v", x, exp)
		}
	}

	var updated []User
	var updatedIDs []int
	n, err = q().FilterID(u0.ID).Gather(&updated).GatherIDs(&updatedIDs).UpdateNonzero(User{Name: "mjl2"})
	tcompare(t, err, n, 1, "update count")
	compareID(u0.ID, User{u0.ID, "mjl2", 0})
	tcompare(t, err, updated, []User{{u0.ID, "mjl2", 0}}, "updated gathered")
	tcompare(t, err, updatedIDs, []int{u0.ID}, "updated gathered ids")

	_, err = q().UpdateNonzero(User{ID: 123, Name: "mjl2"})
	tneed(t, err, ErrParam, "attempt to update PK")

	_, err = q().UpdateNonzero(User{})
	tneed(t, err, ErrParam, "no nonzero fields")

	err = q().Gather(nil).Err()
	tneed(t, err, ErrParam, "nil list")

	err = q().Gather(&updated).Gather(&updated).Err()
	tneed(t, err, ErrParam, "duplicate gather")

	err = q().GatherIDs(nil).Err()
	tneed(t, err, ErrParam, "nil list")

	err = q().GatherIDs(&updatedIDs).GatherIDs(&updatedIDs).Err()
	tneed(t, err, ErrParam, "duplicate gatherIDs")

	uids := []uint{}
	err = q().GatherIDs(&uids).Err()
	tneed(t, err, ErrParam, "wrong type for gatherIDs")

	n, err = q().Count()
	tcompare(t, err, n, 2, "count all")

	_, err = q().FilterNonzero(User{Name: "mjl2"}).Get()
	tcheck(t, err, "match get")

	_, err = q().FilterEqual("Name", "mjl2").List()
	tcheck(t, err, "field list")

	_, err = q().FilterNonzero(User{Name: "mjl2"}).Limit(1).Get()
	tcheck(t, err, "limit get")

	n, err = q().Limit(1).Count()
	tcompare(t, err, n, 1, "limit count")

	var ids []int
	err = q().SortDesc("ID").IDs(&ids)
	tcompare(t, err, ids, []int{u1.ID, u0.ID}, "sort id desc")

	ids = nil
	err = q().SortAsc("ID").IDs(&ids)
	tcompare(t, err, ids, []int{u0.ID, u1.ID}, "sort id asc")

	ids = nil
	err = q().SortDesc("Name").IDs(&ids)
	tcompare(t, err, ids, []int{u0.ID, u1.ID}, "sort name desc")

	ids = nil
	err = q().SortAsc("Name").IDs(&ids)
	tcompare(t, err, ids, []int{u1.ID, u0.ID}, "sort name asc")

	ids = nil
	err = q().SortAsc("ID").IDs(&ids)
	tcompare(t, err, ids, []int{u0.ID, u1.ID}, "sort id asc")

	ids = nil
	err = q().FilterGreater("ID", 0).SortAsc("ID").IDs(&ids)
	tcompare(t, err, ids, []int{u0.ID, u1.ID}, "sort id asc")

	err = q().SortAsc().Err()
	tneed(t, err, ErrParam, "sort without fields")

	ok, err := q().FilterNonzero(User{Name: "mjl2"}).Exists()
	tcompare(t, err, ok, true, "exists existing")

	ok, err = q().FilterNonzero(User{Name: "bogus"}).Exists()
	tcompare(t, err, ok, false, "exists nonexisting")

	n, err = q().FilterFn(func(v User) bool {
		return v.Name == "mjl2"
	}).Count()
	tcompare(t, err, n, 1, "single count")

	err = q().FilterFn(nil).Err()
	tneed(t, err, ErrParam, "filterfn with nil fn")

	err = db.Read(func(tx *Tx) error {
		n, err = QueryTx[User](tx).Count()
		tcompare(t, err, n, 2, "count all in transaction")
		return nil
	})
	tcheck(t, err, "read")

	n, err = q().FilterID(u0.ID).UpdateField("Num", 10)
	tcompare(t, err, n, 1, "update single")

	_, err = q().FilterID(u0.ID).UpdateField("Num", nil)
	tneed(t, err, ErrParam, "update invalid nil")

	err = db.Get(&u0)
	tcheck(t, err, "get u0")

	n, err = q().FilterID(u1.ID).UpdateFields(map[string]any{"Num": 30, "Name": "other"})
	tcompare(t, err, n, 1, "update single")

	err = db.Get(&u1)
	tcheck(t, err, "get u1")

	n, err = q().FilterID(u1.ID).UpdateFields(map[string]any{"Num": int32(30)})
	tcompare(t, err, n, 1, "update single with compatible type")

	_, err = q().FilterID(u1.ID).UpdateFields(map[string]any{"Num": int64(30)})
	tneed(t, err, ErrParam, "update single with incompatible type")

	_, err = q().UpdateFields(map[string]any{})
	tneed(t, err, ErrParam, "update no values")

	_, err = q().UpdateFields(map[string]any{"ID": 123})
	tneed(t, err, ErrParam, "update PK")

	_, err = q().UpdateFields(map[string]any{"Num": 1i})
	tneed(t, err, ErrParam, "update bad type")

	_, err = q().UpdateFields(map[string]any{"Bogus": "does not exist"})
	tneed(t, err, ErrParam, "update nonexistent field")

	u2 := User{0, "a", 20}
	err = db.Insert(&u2)
	tcheck(t, err, "insert a")

	err = q().FilterGreaterEqual("Bogus", 1).Err()
	tneed(t, err, ErrParam, "filter compare with unknown field")

	n, err = q().FilterGreaterEqual("Num", 10).FilterLessEqual("Num", 30).Count()
	tcompare(t, err, n, 3, "greaterequal lessequal")

	n, err = q().FilterGreater("Num", 10).FilterLess("Num", 30).Count()
	tcompare(t, err, n, 1, "greater less")

	nums := []any{int8(30), int16(30), int32(30), int(30)}
	for _, num := range nums {
		n, err = q().FilterGreater("Num", 10).FilterLess("Num", num).Count()
		tcompare(t, err, n, 1, "greater less")
	}
	n, err = q().FilterGreater("Num", 10).FilterLess("Num", int64(30)).Count()
	tneed(t, err, ErrParam, "int64 for compare to int")

	n, err = q().FilterEqual("Name", "mjl2", "other").Count()
	tcompare(t, err, n, 2, "filterIn with string")

	err = q().FilterGreater("Num", nil).Err()
	tneed(t, err, ErrParam, "nil for num int")

	err = q().FilterEqual("Num", nil).Err()
	tneed(t, err, ErrParam, "nil for num int")

	n = 0
	err = q().ForEach(func(v User) error {
		n++
		return nil
	})
	tcompare(t, err, n, 3, "foreach")

	xerr := errors.New("error")
	err = q().ForEach(func(v User) error {
		return xerr
	})
	tneed(t, err, xerr, "foreach error")

	func() {
		defer func() {
			x := recover()
			if x != xerr {
				t.Fatalf("recover: got %v, expected %v", x, xerr)
			}
		}()
		err = q().ForEach(func(v User) error {
			panic(xerr)
		})
		t.Fatalf("missing panic")
	}()

	n = 0
	err = q().FilterNonzero(User{Name: "bogus"}).ForEach(func(v User) error {
		n++
		return nil
	})
	tcompare(t, err, n, 0, "foreach")

	ids = nil
	err = q().IDs(&ids)
	tcompare(t, err, ids, []int{u0.ID, u1.ID, u2.ID}, "ids")

	err = q().IDs(nil)
	tneed(t, err, ErrParam, "nil ids")

	err = q().IDs(&uids)
	tneed(t, err, ErrParam, "bad type for ids")

	n = 0
	nq := q()
	for {
		_, err := nq.Next()
		if err == ErrAbsent {
			break
		}
		tcheck(t, err, "next")
		n++
	}
	tcompare(t, nil, n, 3, "next")
	err = nq.Close()
	tcheck(t, err, "close after next")

	n = 0
	ids = []int{}
	nq = q()
	err = nq.Err()
	tcheck(t, err, "err")
	for {
		var id int
		err := nq.NextID(&id)
		if err == ErrAbsent {
			break
		}
		tcheck(t, err, "nextid")
		ids = append(ids, id)
	}
	tcompare(t, nil, ids, []int{u0.ID, u1.ID, u2.ID}, "nextid")
	err = nq.Err()
	tneed(t, err, ErrAbsent, "err after iteration")
	err = nq.Close()
	tcheck(t, err, "q close")
	err = nq.Err()
	tneed(t, err, ErrFinished, "err after close")

	err = q().NextID(nil)
	tneed(t, err, ErrParam, "nil idptr")

	var uid uint
	err = q().NextID(&uid)
	tneed(t, err, ErrParam, "uint id")

	ids = nil
	err = q().FilterIDs([]int{u1.ID, u0.ID}).IDs(&ids)
	tcompare(t, err, ids, []int{u1.ID, u0.ID}, "ids after filterIDs")

	ids = nil
	err = q().FilterIDs([]int{u0.ID, u1.ID}).SortDesc("Name").IDs(&ids)
	tcompare(t, err, ids, []int{u1.ID, u0.ID}, "filter ids with in-memory sort")

	users, err := q().SortDesc("Name").List()
	tcompare(t, err, users, []User{u1, u0, u2}, "list with in-memory sort")

	err = q().FilterIDs(nil).Err()
	tneed(t, err, ErrParam, "nil ids")

	err = q().FilterIDs([]uint{1}).Err()
	tneed(t, err, ErrParam, "uint for id")

	n, err = q().FilterIDs([]int{u0.ID}).FilterIDs([]int{u1.ID}).Count()
	tcompare(t, err, n, 0, "filter on two different ids")

	n, err = q().FilterIDs([]int{u1.ID, u0.ID}).FilterIDs([]int{u1.ID}).Count()
	tcompare(t, err, n, 1, "refine filterIDs")

	n, err = q().FilterIDs([]int{u1.ID, u0.ID}).FilterIDs([]int{u0.ID, u1.ID}).Count()
	tcompare(t, err, n, 2, "refine filterIDs")

	n, err = q().FilterIDs([]int{u0.ID}).FilterIDs([]int{u0.ID, u1.ID}).Count()
	tcompare(t, err, n, 1, "refine filterIDs")

	n, err = q().FilterID(u0.ID).FilterID(u0.ID).Count()
	tcompare(t, err, n, 1, "refine filter id")

	n, err = q().FilterID(u0.ID).FilterID(u1.ID).Count()
	tcompare(t, err, n, 0, "refine filter id")

	n, err = q().FilterIDs([]int{u0.ID, u1.ID}).FilterID(u1.ID).Count()
	tcompare(t, err, n, 1, "refine filter id")

	n, err = q().FilterIDs([]int{u0.ID, u1.ID}).FilterEqual("Num", 50).Count()
	tcompare(t, err, n, 0, "refine filterIn with additional filter")

	ids = nil
	err = q().FilterIDs([]int{u1.ID, u0.ID}).SortAsc("ID").IDs(&ids)
	tcompare(t, err, ids, []int{u0.ID, u1.ID}, "ids after filterIDs asc")

	n, err = q().FilterGreaterEqual("Num", 0).FilterLessEqual("Num", 20).Delete()
	tcompare(t, err, n, 2, "delete two records")

	ok, err = q().FilterNonzero(User{Num: 30}).Exists()
	tcompare(t, err, ok, true, "exists")

	err = q().FilterNonzero(User{}).Err()
	tneed(t, err, ErrParam, "nonzero without fields")

	err = (&Query[User]{}).Err()
	tneed(t, err, ErrParam, "query without tx/db")
}

func TestQueryTime(t *testing.T) {
	type User struct {
		ID   int
		Name string
		Num  int
		Time time.Time `bstore:"index"`
	}

	os.Remove("testdata/querytime.db")
	db, err := topen(t, "testdata/querytime.db", nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	now := time.Now()
	u0 := User{Name: "mjl", Time: now}
	err = db.Insert(&u0)
	tcheck(t, err, "insert u0")

	u1 := User{Name: "gopher", Time: now.Add(-time.Minute)}
	err = db.Insert(&u1)
	tcheck(t, err, "insert u1")

	// Strip monotonic time part for comparison below.
	now = now.Round(0)
	err = db.Get(&u0)
	tcheck(t, err, "get u0")
	err = db.Get(&u1)
	tcheck(t, err, "get u1")

	q := func() *Query[User] {
		return QueryDB[User](db)
	}

	var ids []int

	err = q().SortAsc("Time").IDs(&ids)
	tcompare(t, err, ids, []int{u1.ID, u0.ID}, "order asci ids")

	x0, err := q().FilterGreaterEqual("Time", now).Get()
	tcompare(t, err, x0, u0, "filter time >=")
}

func TestQueryUnique(t *testing.T) {
	type User struct {
		ID   int
		Name string `bstore:"unique"`
		Num  int
		Time time.Time `bstore:"index"`
	}

	os.Remove("testdata/queryunique.db")
	db, err := topen(t, "testdata/queryunique.db", nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	now := time.Now().Round(0)
	u0 := User{Name: "mjl", Time: now}
	err = db.Insert(&u0)
	tcheck(t, err, "insert u0")

	x0, err := QueryDB[User](db).FilterEqual("Name", "mjl").Get()
	tcompare(t, err, x0, u0, "compare")

	x0, err = QueryDB[User](db).FilterEqual("Name", "mjl", "mjl2").Get()
	tcompare(t, err, x0, u0, "compare")
}

func TestQueryUniqueMulti(t *testing.T) {
	type User struct {
		ID   int
		Name string `bstore:"unique Name+Role"`
		Role string
	}

	os.Remove("testdata/queryuniquemulti.db")
	db, err := topen(t, "testdata/queryuniquemulti.db", nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	u0 := User{Name: "mjl", Role: "tester"}
	err = db.Insert(&u0)
	tcheck(t, err, "insert u0")

	x0, err := QueryDB[User](db).FilterEqual("Name", "mjl").FilterEqual("Role", "tester").Get()
	tcompare(t, err, x0, u0, "compare")
}

func TestQueryRangeIndex(t *testing.T) {
	type Msg struct {
		ID        int64
		MailboxID uint32 `bstore:"unique MailboxID+UID,index MailboxID+Received,ref Mailbox"`
		UID       uint32
		Received  time.Time
	}

	type Mailbox struct {
		ID   uint32
		Name string `bstore:"unique"`
	}

	os.Remove("testdata/queryrangeindex.db")
	db, err := topen(t, "testdata/queryrangeindex.db", nil, Msg{}, Mailbox{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	mb0 := Mailbox{Name: "INBOX"}
	err = db.Insert(&mb0)
	tcheck(t, err, "insert mailbox")

	tm0 := time.Now().Round(0)
	m0 := Msg{UID: 1, MailboxID: mb0.ID, Received: tm0}
	err = db.Insert(&m0)
	tcheck(t, err, "insert m0")

	tm1 := time.Now().Round(0)
	m1 := Msg{UID: 2, MailboxID: mb0.ID, Received: tm1}
	err = db.Insert(&m1)
	tcheck(t, err, "m1")

	q := func() *Query[Msg] {
		return QueryDB[Msg](db)
	}

	l, err := q().List()
	tcompare(t, err, l, []Msg{m0, m1}, "list messages")

	var stats, delta Stats
	stats = db.Stats()

	updateStats := func() {
		nstats := db.Stats()
		delta = nstats.Sub(stats)
		stats = nstats
	}

	x0, err := q().FilterEqual("UID", m0.UID).FilterEqual("MailboxID", mb0.ID).Get()
	tcompare(t, err, x0, m0, "get m0 with index")
	updateStats()
	tcompare(t, err, delta.PlanUnique, uint(1), "plan unique")

	l, err = q().FilterEqual("UID", m0.UID, m1.UID).FilterEqual("MailboxID", mb0.ID).List()
	tcompare(t, err, l, []Msg{m0, m1}, "get m0, m1 with index")
	updateStats()
	tcompare(t, err, delta.PlanUnique, uint(1), "plan unique")

	var ids []int64
	err = q().FilterEqual("MailboxID", mb0.ID).IDs(&ids)
	tcompare(t, err, ids, []int64{m0.ID, m1.ID}, "msg ids with index")
	updateStats()
	tcompare(t, err, delta.PlanIndexScan, uint(1), "plan index scan")
	tcompare(t, err, delta.LastOrdered, true, "ordered plan")
	tcompare(t, err, delta.LastAsc, true, "ascending")

	l, err = q().FilterEqual("MailboxID", mb0.ID).SortDesc("Received").List()
	tcompare(t, err, l, []Msg{m1, m0}, "list messages by received desc")
	updateStats()
	tcompare(t, err, delta.PlanIndexScan, uint(1), "plan index scan")
	tcompare(t, err, delta.LastOrdered, true, "ordered plan")
	tcompare(t, err, delta.LastAsc, false, "descending")

	ids = nil
	err = q().FilterGreater("Received", m0.Received).IDs(&ids)
	tcompare(t, err, ids, []int64{m1.ID}, "ids after m0.received")
	updateStats()
	tcompare(t, err, delta.PlanTableScan, uint(1), "plan table scan")
	tcompare(t, err, delta.PlanIndexScan, uint(0), "plan index scan")

	ids = nil
	err = q().FilterEqual("MailboxID", mb0.ID).FilterGreater("Received", m0.Received).IDs(&ids)
	tcompare(t, err, ids, []int64{m1.ID}, "ids after m0.received")
	updateStats()
	tcompare(t, err, delta.PlanIndexScan, uint(1), "plan index scan")
	tcompare(t, err, delta.LastOrdered, true, "ordered plan")
	tcompare(t, err, delta.LastAsc, true, "direction")

	ids = nil
	err = q().FilterEqual("MailboxID", mb0.ID).FilterGreaterEqual("Received", m0.Received).IDs(&ids)
	tcompare(t, err, ids, []int64{m0.ID, m1.ID}, "ids >= m0.received")
	updateStats()
	tcompare(t, err, delta.PlanIndexScan, uint(1), "plan index scan")
	tcompare(t, err, delta.LastOrdered, true, "ordered plan")
	tcompare(t, err, delta.LastAsc, true, "direction")

	ids = nil
	err = q().FilterEqual("MailboxID", mb0.ID).FilterLess("Received", m1.Received).IDs(&ids)
	tcompare(t, err, ids, []int64{m0.ID}, "ids < m1.received")
	updateStats()
	tcompare(t, err, delta.PlanIndexScan, uint(1), "plan index scan")
	tcompare(t, err, delta.LastOrdered, true, "ordered plan")
	tcompare(t, err, delta.LastAsc, true, "direction")

	ids = nil
	err = q().FilterEqual("MailboxID", mb0.ID).FilterLessEqual("Received", m1.Received).SortAsc("Received").IDs(&ids)
	tcompare(t, err, ids, []int64{m0.ID, m1.ID}, "ids < m1.received sort received asc")
	updateStats()
	tcompare(t, err, delta.PlanIndexScan, uint(1), "plan index scan")
	tcompare(t, err, delta.Sort, uint(0), "no sorting")
	tcompare(t, err, delta.LastOrdered, true, "ordered plan")
	tcompare(t, err, delta.LastAsc, true, "direction")

	ids = nil
	err = q().SortAsc("Received").IDs(&ids)
	tcompare(t, err, ids, []int64{m0.ID, m1.ID}, "ids sort received asc")
	updateStats()
	tcompare(t, err, delta.PlanTableScan, uint(1), "plan tablescan")
	tcompare(t, err, delta.Sort, uint(1), "with sorting")
}

func TestNegative(t *testing.T) {
	type Stats struct {
		ID      int
		Celsius int64 `bstore:"index"`
	}

	os.Remove("testdata/negative.db")
	db, err := topen(t, "testdata/negative.db", nil, Stats{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	s0 := Stats{Celsius: math.MinInt64}
	s1 := Stats{Celsius: 0}
	s2 := Stats{Celsius: 1}
	s3 := Stats{Celsius: math.MaxInt64}
	err = db.Insert(&s0, &s1, &s2, &s3)
	tcheck(t, err, "insert")

	xstats, err := QueryDB[Stats](db).SortAsc("Celsius").List()
	tcompare(t, err, xstats, []Stats{s0, s1, s2, s3}, "list asc by celsius")

	xstats, err = QueryDB[Stats](db).SortDesc("Celsius").List()
	tcompare(t, err, xstats, []Stats{s3, s2, s1, s0}, "list desc by celsius")
}

func TestQueryLimit(t *testing.T) {
	type User struct {
		ID int
	}

	os.Remove("testdata/querylimit.db")
	db, err := topen(t, "testdata/querylimit.db", nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	err = QueryDB[User](db).Limit(1).Limit(1).Err()
	tneed(t, err, ErrParam, "dup limit")

	err = QueryDB[User](db).Limit(0).Err()
	tneed(t, err, ErrParam, "limit 0")

	err = QueryDB[User](db).Limit(-1).Err()
	tneed(t, err, ErrParam, "limit negative")
}

func TestQueryNotNext(t *testing.T) {
	type User struct {
		ID int
	}

	os.Remove("testdata/querylimit.db")
	db, err := topen(t, "testdata/querylimit.db", nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	err = db.Insert(&User{})
	tcheck(t, err, "insert")

	q := QueryDB[User](db)
	_, err = q.Next()
	tcheck(t, err, "next")

	_, err = q.List()
	tneed(t, err, ErrParam, "calling non-next operation after having called Next/NextID")

	err = q.Err()
	tneed(t, err, ErrFinished, "query now done")

	_, err = q.List()
	tneed(t, err, ErrFinished, "query is done")
}

func TestQueryIncr(t *testing.T) {
	type User struct {
		ID  int
		Num uint32 `bstore:"index"`
	}

	os.Remove("testdata/queryincr.db")
	db, err := topen(t, "testdata/queryincr.db", nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	u0 := User{Num: 1}
	u1 := User{Num: math.MaxUint32}
	u2 := User{Num: 50}
	err = db.Insert(&u0, &u1, &u2)
	tcheck(t, err, "insert users")

	users, err := QueryDB[User](db).FilterLessEqual("Num", uint32(math.MaxUint32)).SortDesc("Num").List()
	tcompare(t, err, users, []User{u1, u2, u0}, "reverse order and filter to test incr()")
}

func TestQueryCompare(t *testing.T) {
	type Struct struct {
		Field int
	}
	type User struct {
		ID     int
		OK     bool
		Data   []byte
		Uint   uint16
		Float  float32
		Slice  []string
		Struct Struct
		Map    map[string]int
	}

	os.Remove("testdata/querycompare.db")
	db, err := topen(t, "testdata/querycompare.db", nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	u0 := User{0, true, []byte("a"), 10, -45.2, nil, Struct{0}, nil}
	u1 := User{0, false, []byte("zzz"), 100, -12.2, nil, Struct{0}, nil}
	u2 := User{0, false, []byte("aa"), 10000, 100.2, nil, Struct{0}, nil}
	err = db.Insert(&u0, &u1, &u2)
	tcheck(t, err, "insert users")

	users, err := QueryDB[User](db).FilterEqual("OK", true).List()
	tcompare(t, err, users, []User{u0}, "compare bool")

	users, err = QueryDB[User](db).FilterEqual("Data", []byte("aa")).List()
	tcompare(t, err, users, []User{u2}, "compare []byte")

	users, err = QueryDB[User](db).FilterEqual("Uint", uint16(10)).List()
	tcompare(t, err, users, []User{u0}, "compare uint")

	users, err = QueryDB[User](db).FilterGreater("Float", float32(50.0)).List()
	tcompare(t, err, users, []User{u2}, "compare float")

	users, err = QueryDB[User](db).FilterGreater("OK", false).List()
	tcompare(t, err, users, []User{u0}, "compare bool")

	users, err = QueryDB[User](db).FilterGreater("Data", []byte("aa")).SortAsc("ID").List()
	tcompare(t, err, users, []User{u1}, "compare []byte")

	users, err = QueryDB[User](db).FilterGreater("Uint", uint16(10)).SortAsc("ID").List()
	tcompare(t, err, users, []User{u1, u2}, "compare uint")

	users, err = QueryDB[User](db).FilterGreater("Float", float32(50.0)).List()
	tcompare(t, err, users, []User{u2}, "compare float")

	err = QueryDB[User](db).FilterGreater("Struct", Struct{1}).Err()
	tneed(t, err, ErrParam, "comparison on struct")

	err = QueryDB[User](db).FilterGreater("Slice", []string{"test"}).Err()
	tneed(t, err, ErrParam, "comparison on slice")

	err = QueryDB[User](db).FilterGreater("Map", map[string]int{"a": 1}).Err()
	tneed(t, err, ErrParam, "comparison on slice")

	err = QueryDB[User](db).SortAsc("Slice").Err()
	tneed(t, err, ErrParam, "sort by slice")

	err = QueryDB[User](db).SortAsc("Struct").Err()
	tneed(t, err, ErrParam, "sort by struct")

	err = QueryDB[User](db).SortAsc("Map").Err()
	tneed(t, err, ErrParam, "sort by map")
}

func TestDeepEqual(t *testing.T) {
	// Test that we don't compare with DeepEqual because it also checks private fields.

	type Struct struct {
		Field   string
		private string
	}

	type User struct {
		ID     int
		Struct Struct
		Slice  []Struct
		Map    map[string]Struct

		private string // Changing this field should not cause an update.
	}

	os.Remove("testdata/deepequal.db")
	db, err := topen(t, "testdata/deepequal.db", nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	var stats, delta Stats
	stats = db.Stats()

	updateStats := func() {
		nstats := db.Stats()
		delta = nstats.Sub(stats)
		stats = nstats
	}

	u0 := User{
		Struct:  Struct{"a", ""},
		Slice:   []Struct{{"a", ""}},
		Map:     map[string]Struct{"key": {"a", ""}},
		private: "",
	}
	err = db.Insert(&u0)
	tcheck(t, err, "insert")
	updateStats()

	u0.private = "modified"
	u0.Struct.private = "modified"
	u0.Slice[0].private = "modified"
	u0.Map["key"] = Struct{"a", "modified"}
	err = db.Update(&u0)
	tcheck(t, err, "update")
	updateStats()
	tcompare(t, nil, delta.Records.Put, uint(0), "puts after update with only private fields changed")

	nv := u0
	nv.ID = 0
	n, err := QueryDB[User](db).UpdateField("Struct", u0.Struct)
	tcompare(t, err, n, 1, "update with query")
	updateStats()
	tcompare(t, nil, delta.Records.Put, uint(0), "puts after update with only private field changed")

	n, err = QueryDB[User](db).FilterEqual("Struct", Struct{"a", "other"}).Count()
	tcompare(t, err, n, 1, "filterequal with other private field")

	n, err = QueryDB[User](db).FilterEqual("Slice", []Struct{{"a", "other"}}, []Struct{{"a", "other2"}}).Count()
	tcompare(t, err, n, 1, "filterin with other private field")

	n, err = QueryDB[User](db).FilterNotEqual("Map", map[string]Struct{"key": {"a", "other"}}).Count()
	tcompare(t, err, n, 0, "filternotequal with other private field")
}

// Check that we don't allow comparison/ordering on pointer fields.
func TestQueryPtr(t *testing.T) {
	type User struct {
		ID   int
		Name *string    `bstore:"default a"`
		Age  *int       `bstore:"default 123"`
		Time *time.Time `bstore:"default now"`
	}

	os.Remove("testdata/queryptr.db")
	db, err := topen(t, "testdata/queryptr.db", nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	u0 := User{}
	err = db.Insert(&u0)
	tcheck(t, err, "insert")

	err = QueryDB[User](db).FilterEqual("Name", "a").Err()
	tneed(t, err, ErrParam, "filter equal name")

	err = QueryDB[User](db).FilterGreater("Name", "a").Err()
	tneed(t, err, ErrParam, "filter equal name")

	err = QueryDB[User](db).FilterEqual("Time", time.Now()).Err()
	tneed(t, err, ErrParam, "filter equal time")

	err = QueryDB[User](db).FilterEqual("Time", nil).Err()
	tneed(t, err, ErrParam, "filter equal nil time")

	err = QueryDB[User](db).SortAsc("Name").Err()
	tneed(t, err, ErrParam, "sort on ptr")

	err = QueryDB[User](db).SortAsc("Time").Err()
	tneed(t, err, ErrParam, "sort on ptr")

	name := "b"
	age := 2
	time := time.Now().Round(0)
	u1 := User{0, &name, &age, &time}
	err = db.Insert(&u1)
	tcheck(t, err, "insert with ptrs")

	var stats, delta Stats
	stats = db.Stats()

	updateStats := func() {
		nstats := db.Stats()
		delta = nstats.Sub(stats)
		stats = nstats
	}

	err = db.Update(&u1)
	tcheck(t, err, "update")
	updateStats()
	tcompare(t, err, delta.Records.Put, uint(0), "puts after update without changes with ptr fields")

	name2 := "b"
	age2 := 2
	u1.Name = &name2
	u1.Age = &age2
	err = db.Update(&u1)
	updateStats()
	tcompare(t, err, delta.Records.Put, uint(0), "puts after update with different ptrs but same values")

	name3 := "c"
	age3 := 3
	u1.Name = &name3
	u1.Age = &age3
	err = db.Update(&u1)
	updateStats()
	tcompare(t, err, delta.Records.Put, uint(1), "puts after update with different ptrs and different values")

	x1, err := QueryDB[User](db).FilterID(u1.ID).Get()
	tcompare(t, err, x1, u1, "get after update")

	u1.Name = nil
	err = db.Update(&u1)
	updateStats()
	tcompare(t, err, delta.Records.Put, uint(1), "puts after update with different ptrs and different values")
	x1, err = QueryDB[User](db).FilterID(u1.ID).Get()
	tcompare(t, err, x1, u1, "get after update")

	u1.Age = nil
	n, err := QueryDB[User](db).FilterID(u1.ID).UpdateField("Age", u1.Age)
	tcompare(t, err, n, 1, "update age to nil")

	x1, err = QueryDB[User](db).FilterID(u1.ID).Get()
	tcompare(t, err, x1, u1, "get after update")

	u1.Age = &age3
	n, err = QueryDB[User](db).FilterID(u1.ID).UpdateField("Age", u1.Age)
	tcompare(t, err, n, 1, "update age to non-nil")

	x1, err = QueryDB[User](db).FilterID(u1.ID).Get()
	tcompare(t, err, x1, u1, "get after update")

	n, err = QueryDB[User](db).FilterID(u1.ID).UpdateField("Age", 4)
	tcompare(t, err, n, 1, "update age to non-ptr int")
	age4 := 4
	u1.Age = &age4

	x1, err = QueryDB[User](db).FilterID(u1.ID).Get()
	tcompare(t, err, x1, u1, "get after update")

	n, err = QueryDB[User](db).FilterID(u1.ID).UpdateField("Age", int16(5))
	tcompare(t, err, n, 1, "update age to non-ptr int16 that will be converted to int")
	age5 := 5
	u1.Age = &age5

	x1, err = QueryDB[User](db).FilterID(u1.ID).Get()
	tcompare(t, err, x1, u1, "get after update")
}

func TestEmbed(t *testing.T) {
	type Flags struct {
		Seen bool
		Junk bool
	}
	type Msg struct {
		ID int
		Flags
		Name string
	}

	path := "testdata/embed.db"
	os.Remove(path)
	db, err := topen(t, path, nil, Msg{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	m0 := Msg{0, Flags{true, false}, "name"}
	err = db.Insert(&m0)
	tcheck(t, err, "insert")

	x0 := Msg{ID: m0.ID}
	err = db.Get(&x0)
	tcompare(t, err, x0, m0, "get after insert, with embed")

	n, err := QueryDB[Msg](db).FilterEqual("Name", "name").Count()
	tcompare(t, err, n, 1, "filter embedded field seen")

	n, err = QueryDB[Msg](db).UpdateNonzero(Msg{Flags: Flags{false, true}})
	tcompare(t, err, n, 1, "updatenonzero for embed field")

	// Flags is not treated as non-zero struct, but the individual fields are compared, so Seen is not cleared, but Junk is set.
	m0.Flags = Flags{true, true}
	err = db.Get(&x0)
	tcompare(t, err, x0, m0, "get after updatenonzero, with embed")

	n, err = QueryDB[Msg](db).UpdateFields(map[string]any{"Seen": true, "Junk": false})
	tcompare(t, err, n, 1, "updatefields for embedded fields")

	m0.Flags = Flags{true, false}
	x0 = Msg{ID: m0.ID}
	err = db.Get(&x0)
	tcompare(t, err, x0, m0, "get after updatefields, with embed")

	n, err = QueryDB[Msg](db).UpdateField("Flags", Flags{true, false})
	tcompare(t, err, n, 1, "updatefield for  field")

	m0.Flags = Flags{true, false}
	err = db.Get(&x0)
	tcompare(t, err, x0, m0, "get after updatefield, with embed")
}

// Test that types with underlying supported types are handled properly.
func TestCompareKinds(t *testing.T) {
	type UID uint32
	type Msg struct {
		ID        int64
		MailboxID uint32
		UID       UID
	}

	path := "testdata/comparekinds.db"
	os.Remove(path)
	db, err := topen(t, path, nil, Msg{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	m0 := Msg{UID: 1}
	m1 := Msg{UID: 100}
	err = db.Insert(&m0, &m1)
	tcheck(t, err, "insert")

	n, err := QueryDB[Msg](db).FilterLess("UID", UID(10)).Count()
	tcompare(t, err, n, 1, "filterless with type with underlying supported kind")

	msgs, err := QueryDB[Msg](db).SortDesc("UID").List()
	tcompare(t, err, msgs, []Msg{m1, m0}, "list desc")
}

// Test proper start/stop filtering for index on string field.
// Strings have variable width index key width.
func TestStringIndex(t *testing.T) {
	type User struct {
		ID   int
		Name string `bstore:"unique"`
	}

	path := "testdata/stringindex.db"
	os.Remove(path)
	db, err := topen(t, path, nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	u0 := User{Name: "z"}
	u1 := User{Name: "zz"}
	u2 := User{Name: "zzz"}

	err = db.Insert(&u0, &u1, &u2)
	tcheck(t, err, "insert")

	users, err := QueryDB[User](db).FilterGreater("Name", "z").SortAsc("ID").List()
	tcompare(t, err, users, []User{u1, u2}, "greater")

	users, err = QueryDB[User](db).FilterGreater("Name", "z").FilterLess("Name", "zzz").SortAsc("ID").List()
	tcompare(t, err, users, []User{u1}, "list")

	users, err = QueryDB[User](db).FilterGreater("Name", "zz").FilterLess("Name", "zzzz").List()
	tcompare(t, err, users, []User{u2}, "list")

	users, err = QueryDB[User](db).FilterGreater("Name", "zz").FilterLess("Name", "{").List()
	tcompare(t, err, users, []User{u2}, "list")

	users, err = QueryDB[User](db).FilterGreater("Name", "zz").FilterLess("Name", "zzzz").SortDesc("Name").List()
	tcompare(t, err, users, []User{u2}, "list")

	users, err = QueryDB[User](db).FilterGreater("Name", "zz").FilterLess("Name", "{").SortDesc("Name").List()
	tcompare(t, err, users, []User{u2}, "list")

	users, err = QueryDB[User](db).FilterGreater("Name", "zz").FilterLessEqual("Name", "zzzz").SortDesc("Name").List()
	tcompare(t, err, users, []User{u2}, "list")

	users, err = QueryDB[User](db).FilterGreater("Name", "zz").FilterLessEqual("Name", "{").SortDesc("Name").List()
	tcompare(t, err, users, []User{u2}, "list")

	users, err = QueryDB[User](db).FilterGreaterEqual("Name", "zz").FilterLessEqual("Name", "zzzz").SortDesc("Name").List()
	tcompare(t, err, users, []User{u2, u1}, "list")

	users, err = QueryDB[User](db).FilterGreaterEqual("Name", "zz").FilterLessEqual("Name", "{").SortDesc("Name").List()
	tcompare(t, err, users, []User{u2, u1}, "list")

	users, err = QueryDB[User](db).FilterGreater("Name", "zz").FilterLessEqual("Name", "zzzz").SortAsc("Name").List()
	tcompare(t, err, users, []User{u2}, "list")

	users, err = QueryDB[User](db).FilterGreater("Name", "zz").FilterLessEqual("Name", "{").SortAsc("Name").List()
	tcompare(t, err, users, []User{u2}, "list")

	users, err = QueryDB[User](db).FilterGreaterEqual("Name", "zz").FilterLessEqual("Name", "zzzz").SortAsc("Name").List()
	tcompare(t, err, users, []User{u1, u2}, "list")

	users, err = QueryDB[User](db).FilterGreaterEqual("Name", "zz").FilterLessEqual("Name", "{").SortAsc("Name").List()
	tcompare(t, err, users, []User{u1, u2}, "list")

	users, err = QueryDB[User](db).FilterGreaterEqual("Name", "").FilterLessEqual("Name", "{").SortAsc("Name").List()
	tcompare(t, err, users, []User{u0, u1, u2}, "list")

	users, err = QueryDB[User](db).FilterGreaterEqual("Name", "").FilterLessEqual("Name", "{").SortDesc("Name").List()
	tcompare(t, err, users, []User{u2, u1, u0}, "list")

	users, err = QueryDB[User](db).FilterGreaterEqual("Name", "zz").FilterLessEqual("Name", "zz").SortDesc("Name").List()
	tcompare(t, err, users, []User{u1}, "list")

	users, err = QueryDB[User](db).FilterGreaterEqual("Name", "zz").FilterLessEqual("Name", "zz").SortAsc("Name").List()
	tcompare(t, err, users, []User{u1}, "list")
}

func TestIDsLimit(t *testing.T) {
	type User struct {
		ID    int
		Name  string `bstore:"unique"`
		Text  string `bstore:"index"`
		Other string
	}

	path := "testdata/idslimit.db"
	os.Remove(path)
	db, err := topen(t, path, nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	u0 := User{0, "a", "a", "a"}
	u1 := User{0, "b", "b", "b"}
	err = db.Insert(&u0, &u1)
	tcheck(t, err, "insert")

	// By ID.
	users, err := QueryDB[User](db).FilterIDs([]int{u0.ID, u1.ID}).Limit(1).List()
	tcompare(t, err, users, []User{u0}, "filterids with limit")

	users, err = QueryDB[User](db).FilterIDs([]int{u0.ID, u1.ID}).Limit(1).SortDesc("Name").List()
	tcompare(t, err, users, []User{u1}, "filterids with limit with sort")

	users, err = QueryDB[User](db).FilterIDs([]int{u0.ID, u1.ID}).Limit(1).SortDesc("ID").List()
	tcompare(t, err, users, []User{u1}, "filterids with limit with sort")

	users, err = QueryDB[User](db).FilterIDs([]int{u0.ID, u1.ID}).Limit(1).SortAsc("ID").List()
	tcompare(t, err, users, []User{u0}, "filterids with limit with sort")

	// By Name, unique index.
	users, err = QueryDB[User](db).FilterEqual("Name", "a", "b").Limit(1).List()
	tcompare(t, err, users, []User{u0}, "filterids with limit")

	users, err = QueryDB[User](db).FilterEqual("Name", "a", "b").Limit(1).SortDesc("Name").List()
	tcompare(t, err, users, []User{u1}, "filterids with limit with sort")

	users, err = QueryDB[User](db).FilterEqual("Name", "a", "b").Limit(1).SortDesc("ID").List()
	tcompare(t, err, users, []User{u1}, "filterids with limit with sort")

	users, err = QueryDB[User](db).FilterEqual("Name", "a", "b").Limit(1).SortAsc("ID").List()
	tcompare(t, err, users, []User{u0}, "filterids with limit with sort")

	// By Text, regular index.
	users, err = QueryDB[User](db).FilterEqual("Text", "a", "b").Limit(1).List()
	tcompare(t, err, users, []User{u0}, "filterids with limit")

	users, err = QueryDB[User](db).FilterEqual("Text", "a", "b").Limit(1).SortDesc("Text").List()
	tcompare(t, err, users, []User{u1}, "filterids with limit with sort")

	users, err = QueryDB[User](db).FilterEqual("Text", "a", "b").Limit(1).SortDesc("ID").List()
	tcompare(t, err, users, []User{u1}, "filterids with limit with sort")

	users, err = QueryDB[User](db).FilterEqual("Text", "a", "b").Limit(1).SortAsc("ID").List()
	tcompare(t, err, users, []User{u0}, "filterids with limit with sort")

	// By Other, no index.
	users, err = QueryDB[User](db).FilterEqual("Other", "a", "b").Limit(1).List()
	tcompare(t, err, users, []User{u0}, "filterids with limit")

	users, err = QueryDB[User](db).FilterEqual("Other", "a", "b").Limit(1).SortDesc("Other").List()
	tcompare(t, err, users, []User{u1}, "filterids with limit with sort")

	users, err = QueryDB[User](db).FilterEqual("Other", "a", "b").Limit(1).SortDesc("ID").List()
	tcompare(t, err, users, []User{u1}, "filterids with limit with sort")

	users, err = QueryDB[User](db).FilterEqual("Other", "a", "b").Limit(1).SortAsc("ID").List()
	tcompare(t, err, users, []User{u0}, "filterids with limit with sort")
}

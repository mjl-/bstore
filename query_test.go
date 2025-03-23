package bstore

import (
	"context"
	"errors"
	"math"
	"os"
	"slices"
	"testing"
	"time"
)

var ctxbg = context.Background()

func TestQuery(t *testing.T) {
	type User struct {
		ID   int
		Name string
		Num  int
	}

	const path = "testdata/tmp.query.db"
	os.Remove(path)
	db, err := topen(t, path, nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	u0 := User{Name: "mjl"}
	err = db.Insert(ctxbg, &u0)
	tcheck(t, err, "insert u0")

	u1 := User{Name: "gopher"}
	err = db.Insert(ctxbg, &u1)
	tcheck(t, err, "insert u1")

	err = QueryDB[*User](ctxbg, db).Err()
	tneed(t, err, ErrType, "pointer type")

	q := func() *Query[User] {
		return QueryDB[User](ctxbg, db)
	}

	x0, err := q().FilterID(u0.ID).Get()
	tcompare(t, err, x0, u0, "get x0")

	x0, err = q().FilterID(u0.ID).SortAsc("ID").Get()
	tcompare(t, err, x0, u0, "get x0")

	_, err = q().FilterID(u0.ID + 999).Get()
	tneed(t, err, ErrAbsent, "get for nonexistent record")

	_, err = q().Get()
	tneed(t, err, ErrMultiple, "get with multiple selected records")

	x0, err = q().FilterNonzero(User{ID: u0.ID}).Get()
	tcompare(t, err, x0, u0, "get x0")

	lexp := []User{u0, u1}
	l, err := q().List()
	tcompare(t, err, l, lexp, "list")

	l, err = q().FilterNotEqual("ID", u0.ID, u1.ID).List()
	tcompare(t, err, l, []User{}, "empty selection returns empty list, not nil")

	x0, err = q().FilterEqual("ID", u0.ID).Get()
	tcompare(t, err, x0, u0, "get x0")

	x1, err := q().FilterNotEqual("ID", u0.ID).Get()
	tcompare(t, err, x1, u1, "get not u0")

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
		err := db.Get(ctxbg, &x)
		tcompare(t, err, x, exp, "get user")
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

	err = db.Read(ctxbg, func(tx *Tx) error {
		n, err = QueryTx[User](tx).Count()
		tcompare(t, err, n, 2, "count all in transaction")
		return nil
	})
	tcheck(t, err, "read")

	n, err = q().FilterID(u0.ID).UpdateField("Num", 10)
	tcompare(t, err, n, 1, "update single")

	_, err = q().FilterID(u0.ID).UpdateField("Num", nil)
	tneed(t, err, ErrParam, "update invalid nil")

	err = db.Get(ctxbg, &u0)
	tcheck(t, err, "get u0")

	n, err = q().FilterID(u1.ID).UpdateFields(map[string]any{"Num": 30, "Name": "other"})
	tcompare(t, err, n, 1, "update single")

	err = db.Get(ctxbg, &u1)
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
	err = db.Insert(ctxbg, &u2)
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

	n = 0
	err = q().ForEach(func(v User) error {
		n++
		return StopForEach
	})
	tcompare(t, err, n, 1, "foreach with stop")

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

	const path = "testdata/tmp.querytime.db"
	os.Remove(path)
	db, err := topen(t, path, nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	now := time.Now()
	u0 := User{Name: "mjl", Time: now}
	err = db.Insert(ctxbg, &u0)
	tcheck(t, err, "insert u0")

	u1 := User{Name: "gopher", Time: now.Add(-time.Minute)}
	err = db.Insert(ctxbg, &u1)
	tcheck(t, err, "insert u1")

	// Strip monotonic time part for comparison below.
	now = now.Round(0)
	err = db.Get(ctxbg, &u0)
	tcompare(t, err, u0, User{u0.ID, "mjl", 0, now}, "get u0")
	err = db.Get(ctxbg, &u1)
	tcompare(t, err, u1, User{u1.ID, "gopher", 0, u1.Time}, "get u1")

	q := func() *Query[User] {
		return QueryDB[User](ctxbg, db)
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

	const path = "testdata/tmp.queryunique.db"
	os.Remove(path)
	db, err := topen(t, path, nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	now := time.Now().Round(0)
	u0 := User{Name: "mjl", Time: now}
	err = db.Insert(ctxbg, &u0)
	tcheck(t, err, "insert u0")

	x0, err := QueryDB[User](ctxbg, db).FilterEqual("Name", "mjl").Get()
	tcompare(t, err, x0, u0, "compare")

	x0, err = QueryDB[User](ctxbg, db).FilterEqual("Name", "mjl", "mjl2").Get()
	tcompare(t, err, x0, u0, "compare")
}

func TestQueryUniqueMulti(t *testing.T) {
	type User struct {
		ID   int
		Name string `bstore:"unique Name+Role"`
		Role string
	}

	const path = "testdata/tmp.queryuniquemulti.db"
	os.Remove(path)
	db, err := topen(t, path, nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	u0 := User{Name: "mjl", Role: "tester"}
	err = db.Insert(ctxbg, &u0)
	tcheck(t, err, "insert u0")

	x0, err := QueryDB[User](ctxbg, db).FilterEqual("Name", "mjl").FilterEqual("Role", "tester").Get()
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

	const path = "testdata/tmp.queryrangeindex.db"
	os.Remove(path)
	db, err := topen(t, path, nil, Msg{}, Mailbox{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	mb0 := Mailbox{Name: "INBOX"}
	err = db.Insert(ctxbg, &mb0)
	tcheck(t, err, "insert mailbox")

	tm0 := time.Now().Round(0)
	m0 := Msg{UID: 1, MailboxID: mb0.ID, Received: tm0}
	err = db.Insert(ctxbg, &m0)
	tcheck(t, err, "insert m0")

	tm1 := time.Now().Round(0)
	m1 := Msg{UID: 2, MailboxID: mb0.ID, Received: tm1}
	err = db.Insert(ctxbg, &m1)
	tcheck(t, err, "m1")

	q := func() *Query[Msg] {
		return QueryDB[Msg](ctxbg, db)
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

	x0, err = q().FilterNonzero(Msg{ID: m0.ID}).Get()
	tcompare(t, err, x0, m0, "get m0 by primary key through FilterNonzero")
	updateStats()
	tcompare(t, err, delta.PlanPK, uint(1), "plan pk")

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

	const path = "testdata/tmp.negative.db"
	os.Remove(path)
	db, err := topen(t, path, nil, Stats{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	s0 := Stats{Celsius: math.MinInt64}
	s1 := Stats{Celsius: 0}
	s2 := Stats{Celsius: 1}
	s3 := Stats{Celsius: math.MaxInt64}
	err = db.Insert(ctxbg, &s0, &s1, &s2, &s3)
	tcheck(t, err, "insert")

	xstats, err := QueryDB[Stats](ctxbg, db).SortAsc("Celsius").List()
	tcompare(t, err, xstats, []Stats{s0, s1, s2, s3}, "list asc by celsius")

	xstats, err = QueryDB[Stats](ctxbg, db).SortDesc("Celsius").List()
	tcompare(t, err, xstats, []Stats{s3, s2, s1, s0}, "list desc by celsius")
}

func TestQueryLimit(t *testing.T) {
	type User struct {
		ID int
	}

	const path = "testdata/tmp.querylimit.db"
	os.Remove(path)
	db, err := topen(t, path, nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	err = QueryDB[User](ctxbg, db).Limit(1).Limit(1).Err()
	tneed(t, err, ErrParam, "dup limit")

	err = QueryDB[User](ctxbg, db).Limit(0).Err()
	tneed(t, err, ErrParam, "limit 0")

	err = QueryDB[User](ctxbg, db).Limit(-1).Err()
	tneed(t, err, ErrParam, "limit negative")
}

func TestQueryNotNext(t *testing.T) {
	type User struct {
		ID int
	}

	const path = "testdata/tmp.querylimit.db"

	os.Remove(path)
	db, err := topen(t, path, nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	err = db.Insert(ctxbg, &User{})
	tcheck(t, err, "insert")

	q := QueryDB[User](ctxbg, db)
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

	const path = "testdata/tmp.queryincr.db"
	os.Remove(path)
	db, err := topen(t, path, nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	u0 := User{Num: 1}
	u1 := User{Num: math.MaxUint32}
	u2 := User{Num: 50}
	err = db.Insert(ctxbg, &u0, &u1, &u2)
	tcheck(t, err, "insert users")

	users, err := QueryDB[User](ctxbg, db).FilterLessEqual("Num", uint32(math.MaxUint32)).SortDesc("Num").List()
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
		Array  [2]string
	}

	const path = "testdata/tmp.querycompare.db"
	os.Remove(path)
	db, err := topen(t, path, nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	u0 := User{0, true, []byte("a"), 10, -45.2, nil, Struct{0}, nil, [2]string{"", ""}}
	u1 := User{0, false, []byte("zzz"), 100, -12.2, nil, Struct{0}, nil, [2]string{"x", "y"}}
	u2 := User{0, false, []byte("aa"), 10000, 100.2, nil, Struct{0}, nil, [2]string{"string", "other"}}
	err = db.Insert(ctxbg, &u0, &u1, &u2)
	tcheck(t, err, "insert users")

	users, err := QueryDB[User](ctxbg, db).FilterEqual("OK", true).List()
	tcompare(t, err, users, []User{u0}, "compare bool")

	users, err = QueryDB[User](ctxbg, db).FilterEqual("Data", []byte("aa")).List()
	tcompare(t, err, users, []User{u2}, "compare []byte")

	users, err = QueryDB[User](ctxbg, db).FilterEqual("Uint", uint16(10)).List()
	tcompare(t, err, users, []User{u0}, "compare uint")

	users, err = QueryDB[User](ctxbg, db).FilterGreater("Float", float32(50.0)).List()
	tcompare(t, err, users, []User{u2}, "compare float")

	users, err = QueryDB[User](ctxbg, db).FilterGreater("OK", false).List()
	tcompare(t, err, users, []User{u0}, "compare bool")

	users, err = QueryDB[User](ctxbg, db).FilterGreater("Data", []byte("aa")).SortAsc("ID").List()
	tcompare(t, err, users, []User{u1}, "compare []byte")

	users, err = QueryDB[User](ctxbg, db).FilterGreater("Uint", uint16(10)).SortAsc("ID").List()
	tcompare(t, err, users, []User{u1, u2}, "compare uint")

	users, err = QueryDB[User](ctxbg, db).FilterGreater("Float", float32(50.0)).List()
	tcompare(t, err, users, []User{u2}, "compare float")

	users, err = QueryDB[User](ctxbg, db).FilterEqual("Array", [2]string{"", ""}).List()
	tcompare(t, err, users, []User{u0}, "compare array")

	users, err = QueryDB[User](ctxbg, db).FilterEqual("Array", [2]string{"string", "other"}).List()
	tcompare(t, err, users, []User{u2}, "compare array")

	err = QueryDB[User](ctxbg, db).FilterGreater("Struct", Struct{1}).Err()
	tneed(t, err, ErrParam, "comparison on struct")

	err = QueryDB[User](ctxbg, db).FilterGreater("Slice", []string{"test"}).Err()
	tneed(t, err, ErrParam, "comparison on slice")

	err = QueryDB[User](ctxbg, db).FilterGreater("Map", map[string]int{"a": 1}).Err()
	tneed(t, err, ErrParam, "comparison on slice")

	err = QueryDB[User](ctxbg, db).SortAsc("Slice").Err()
	tneed(t, err, ErrParam, "sort by slice")

	err = QueryDB[User](ctxbg, db).SortAsc("Struct").Err()
	tneed(t, err, ErrParam, "sort by struct")

	err = QueryDB[User](ctxbg, db).SortAsc("Map").Err()
	tneed(t, err, ErrParam, "sort by map")
}

func TestDeepEqual(t *testing.T) {
	// Test that we don't compare with DeepEqual because it also checks private fields.

	type Struct struct {
		Field   string
		private string
	}

	type Elem struct {
		S       string
		private string
	}

	type User struct {
		ID     int
		Struct Struct
		Slice  []Struct
		Map    map[string]Struct
		Array  [2]Elem

		private string // Changing this field should not cause an update.
	}

	const path = "testdata/tmp.deepequal.db"
	os.Remove(path)
	db, err := topen(t, path, nil, User{})
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
		Array:   [2]Elem{{"X", "private"}, {"Y", "private"}},
		private: "",
	}
	err = db.Insert(ctxbg, &u0)
	tcheck(t, err, "insert")
	updateStats()

	u0.private = "modified"
	u0.Struct.private = "modified"
	u0.Slice[0].private = "modified"
	u0.Map["key"] = Struct{"a", "modified"}
	u0.Array[1].private = "modified"
	err = db.Update(ctxbg, &u0)
	tcheck(t, err, "update")
	updateStats()
	tcompare(t, nil, delta.Records.Put, uint(0), "puts after update with only private fields changed")

	nv := u0
	nv.ID = 0
	n, err := QueryDB[User](ctxbg, db).UpdateField("Struct", u0.Struct)
	tcompare(t, err, n, 1, "update with query")
	updateStats()
	tcompare(t, nil, delta.Records.Put, uint(0), "puts after update with only private field changed")

	n, err = QueryDB[User](ctxbg, db).FilterEqual("Struct", Struct{"a", "other"}).Count()
	tcompare(t, err, n, 1, "filterequal with struct and other private field")

	n, err = QueryDB[User](ctxbg, db).FilterEqual("Slice", []Struct{{"a", "other"}}, []Struct{{"a", "other2"}}).Count()
	tcompare(t, err, n, 1, "filterin with slice and other private field")

	n, err = QueryDB[User](ctxbg, db).FilterEqual("Array", [2]Elem{{"X", "private"}, {"Y", "other"}}).Count()
	tcompare(t, err, n, 1, "filterequal with array and other private field")

	n, err = QueryDB[User](ctxbg, db).FilterNotEqual("Map", map[string]Struct{"key": {"a", "other"}}).Count()
	tcompare(t, err, n, 0, "filternotequal with map and other private field")
}

// Check that we don't allow comparison/ordering on pointer fields.
func TestQueryPtr(t *testing.T) {
	type User struct {
		ID   int
		Name *string    `bstore:"default a"`
		Age  *int       `bstore:"default 123"`
		Time *time.Time `bstore:"default now"`
	}

	const path = "testdata/tmp.queryptr.db"
	os.Remove(path)
	db, err := topen(t, path, nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	u0 := User{}
	err = db.Insert(ctxbg, &u0)
	tcheck(t, err, "insert")

	err = QueryDB[User](ctxbg, db).FilterEqual("Name", "a").Err()
	tneed(t, err, ErrParam, "filter equal name")

	err = QueryDB[User](ctxbg, db).FilterGreater("Name", "a").Err()
	tneed(t, err, ErrParam, "filter equal name")

	err = QueryDB[User](ctxbg, db).FilterEqual("Time", time.Now()).Err()
	tneed(t, err, ErrParam, "filter equal time")

	err = QueryDB[User](ctxbg, db).FilterEqual("Time", nil).Err()
	tneed(t, err, ErrParam, "filter equal nil time")

	err = QueryDB[User](ctxbg, db).SortAsc("Name").Err()
	tneed(t, err, ErrParam, "sort on ptr")

	err = QueryDB[User](ctxbg, db).SortAsc("Time").Err()
	tneed(t, err, ErrParam, "sort on ptr")

	name := "b"
	age := 2
	time := time.Now().Round(0)
	u1 := User{0, &name, &age, &time}
	err = db.Insert(ctxbg, &u1)
	tcheck(t, err, "insert with ptrs")

	var stats, delta Stats
	stats = db.Stats()

	updateStats := func() {
		nstats := db.Stats()
		delta = nstats.Sub(stats)
		stats = nstats
	}

	err = db.Update(ctxbg, &u1)
	tcheck(t, err, "update")
	updateStats()
	tcompare(t, err, delta.Records.Put, uint(0), "puts after update without changes with ptr fields")

	name2 := "b"
	age2 := 2
	u1.Name = &name2
	u1.Age = &age2
	err = db.Update(ctxbg, &u1)
	updateStats()
	tcompare(t, err, delta.Records.Put, uint(0), "puts after update with different ptrs but same values")

	name3 := "c"
	age3 := 3
	u1.Name = &name3
	u1.Age = &age3
	err = db.Update(ctxbg, &u1)
	updateStats()
	tcompare(t, err, delta.Records.Put, uint(1), "puts after update with different ptrs and different values")

	x1, err := QueryDB[User](ctxbg, db).FilterID(u1.ID).Get()
	tcompare(t, err, x1, u1, "get after update")

	u1.Name = nil
	err = db.Update(ctxbg, &u1)
	updateStats()
	tcompare(t, err, delta.Records.Put, uint(1), "puts after update with different ptrs and different values")
	x1, err = QueryDB[User](ctxbg, db).FilterID(u1.ID).Get()
	tcompare(t, err, x1, u1, "get after update")

	u1.Age = nil
	n, err := QueryDB[User](ctxbg, db).FilterID(u1.ID).UpdateField("Age", u1.Age)
	tcompare(t, err, n, 1, "update age to nil")

	x1, err = QueryDB[User](ctxbg, db).FilterID(u1.ID).Get()
	tcompare(t, err, x1, u1, "get after update")

	u1.Age = &age3
	n, err = QueryDB[User](ctxbg, db).FilterID(u1.ID).UpdateField("Age", u1.Age)
	tcompare(t, err, n, 1, "update age to non-nil")

	x1, err = QueryDB[User](ctxbg, db).FilterID(u1.ID).Get()
	tcompare(t, err, x1, u1, "get after update")

	n, err = QueryDB[User](ctxbg, db).FilterID(u1.ID).UpdateField("Age", 4)
	tcompare(t, err, n, 1, "update age to non-ptr int")
	age4 := 4
	u1.Age = &age4

	x1, err = QueryDB[User](ctxbg, db).FilterID(u1.ID).Get()
	tcompare(t, err, x1, u1, "get after update")

	n, err = QueryDB[User](ctxbg, db).FilterID(u1.ID).UpdateField("Age", int16(5))
	tcompare(t, err, n, 1, "update age to non-ptr int16 that will be converted to int")
	age5 := 5
	u1.Age = &age5

	x1, err = QueryDB[User](ctxbg, db).FilterID(u1.ID).Get()
	tcompare(t, err, x1, u1, "get after update")
}

func TestEmbed(t *testing.T) {
	type Other struct {
		NestedEmbed bool
	}
	type Flags struct {
		Seen  bool
		Junk  bool
		Other Other
	}
	type Msg struct {
		ID   int
		Name string
		Flags
	}

	const path = "testdata/tmp.embed.db"
	os.Remove(path)
	db, err := topen(t, path, nil, Msg{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	m0 := Msg{0, "name", Flags{true, false, Other{true}}}
	err = db.Insert(ctxbg, &m0)
	tcheck(t, err, "insert")

	x0 := Msg{ID: m0.ID}
	err = db.Get(ctxbg, &x0)
	tcompare(t, err, x0, m0, "get after insert, with embed")

	n, err := QueryDB[Msg](ctxbg, db).FilterEqual("Name", "name").Count()
	tcompare(t, err, n, 1, "filter embedded field seen")

	n, err = QueryDB[Msg](ctxbg, db).UpdateNonzero(Msg{Flags: Flags{false, true, Other{true}}})
	tcompare(t, err, n, 1, "updatenonzero for embed field")

	// Flags is not treated as non-zero struct, but the individual fields are compared, so Seen is not cleared, but Junk is set.
	m0.Flags = Flags{true, true, Other{true}}
	err = db.Get(ctxbg, &x0)
	tcompare(t, err, x0, m0, "get after updatenonzero, with embed")

	n, err = QueryDB[Msg](ctxbg, db).UpdateFields(map[string]any{"Seen": true, "Junk": false})
	tcompare(t, err, n, 1, "updatefields for embedded fields")

	m0.Flags = Flags{true, false, Other{true}}
	x0 = Msg{ID: m0.ID}
	err = db.Get(ctxbg, &x0)
	tcompare(t, err, x0, m0, "get after updatefields, with embed")

	n, err = QueryDB[Msg](ctxbg, db).UpdateField("Flags", Flags{true, false, Other{true}})
	tcompare(t, err, n, 1, "updatefield for  field")

	m0.Flags = Flags{true, false, Other{true}}
	err = db.Get(ctxbg, &x0)
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

	const path = "testdata/tmp.comparekinds.db"
	os.Remove(path)
	db, err := topen(t, path, nil, Msg{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	m0 := Msg{UID: 1}
	m1 := Msg{UID: 100}
	err = db.Insert(ctxbg, &m0, &m1)
	tcheck(t, err, "insert")

	n, err := QueryDB[Msg](ctxbg, db).FilterLess("UID", UID(10)).Count()
	tcompare(t, err, n, 1, "filterless with type with underlying supported kind")

	msgs, err := QueryDB[Msg](ctxbg, db).SortDesc("UID").List()
	tcompare(t, err, msgs, []Msg{m1, m0}, "list desc")
}

// Test proper start/stop filtering for index on string field.
// Strings have variable width index key width.
func TestStringIndex(t *testing.T) {
	type User struct {
		ID   int
		Name string `bstore:"unique"`
	}

	const path = "testdata/tmp.stringindex.db"
	os.Remove(path)
	db, err := topen(t, path, nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	u0 := User{Name: "z"}
	u1 := User{Name: "zz"}
	u2 := User{Name: "zzz"}

	err = db.Insert(ctxbg, &u0, &u1, &u2)
	tcheck(t, err, "insert")

	users, err := QueryDB[User](ctxbg, db).FilterGreater("Name", "z").SortAsc("ID").List()
	tcompare(t, err, users, []User{u1, u2}, "greater")

	users, err = QueryDB[User](ctxbg, db).FilterGreater("Name", "z").FilterLess("Name", "zzz").SortAsc("ID").List()
	tcompare(t, err, users, []User{u1}, "list")

	users, err = QueryDB[User](ctxbg, db).FilterGreater("Name", "zz").FilterLess("Name", "zzzz").List()
	tcompare(t, err, users, []User{u2}, "list")

	users, err = QueryDB[User](ctxbg, db).FilterGreater("Name", "zz").FilterLess("Name", "{").List()
	tcompare(t, err, users, []User{u2}, "list")

	users, err = QueryDB[User](ctxbg, db).FilterGreater("Name", "zz").FilterLess("Name", "zzzz").SortDesc("Name").List()
	tcompare(t, err, users, []User{u2}, "list")

	users, err = QueryDB[User](ctxbg, db).FilterGreater("Name", "zz").FilterLess("Name", "{").SortDesc("Name").List()
	tcompare(t, err, users, []User{u2}, "list")

	users, err = QueryDB[User](ctxbg, db).FilterGreater("Name", "zz").FilterLessEqual("Name", "zzzz").SortDesc("Name").List()
	tcompare(t, err, users, []User{u2}, "list")

	users, err = QueryDB[User](ctxbg, db).FilterGreater("Name", "zz").FilterLessEqual("Name", "{").SortDesc("Name").List()
	tcompare(t, err, users, []User{u2}, "list")

	users, err = QueryDB[User](ctxbg, db).FilterGreaterEqual("Name", "zz").FilterLessEqual("Name", "zzzz").SortDesc("Name").List()
	tcompare(t, err, users, []User{u2, u1}, "list")

	users, err = QueryDB[User](ctxbg, db).FilterGreaterEqual("Name", "zz").FilterLessEqual("Name", "{").SortDesc("Name").List()
	tcompare(t, err, users, []User{u2, u1}, "list")

	users, err = QueryDB[User](ctxbg, db).FilterGreater("Name", "zz").FilterLessEqual("Name", "zzzz").SortAsc("Name").List()
	tcompare(t, err, users, []User{u2}, "list")

	users, err = QueryDB[User](ctxbg, db).FilterGreater("Name", "zz").FilterLessEqual("Name", "{").SortAsc("Name").List()
	tcompare(t, err, users, []User{u2}, "list")

	users, err = QueryDB[User](ctxbg, db).FilterGreaterEqual("Name", "zz").FilterLessEqual("Name", "zzzz").SortAsc("Name").List()
	tcompare(t, err, users, []User{u1, u2}, "list")

	users, err = QueryDB[User](ctxbg, db).FilterGreaterEqual("Name", "zz").FilterLessEqual("Name", "{").SortAsc("Name").List()
	tcompare(t, err, users, []User{u1, u2}, "list")

	users, err = QueryDB[User](ctxbg, db).FilterGreaterEqual("Name", "").FilterLessEqual("Name", "{").SortAsc("Name").List()
	tcompare(t, err, users, []User{u0, u1, u2}, "list")

	users, err = QueryDB[User](ctxbg, db).FilterGreaterEqual("Name", "").FilterLessEqual("Name", "{").SortDesc("Name").List()
	tcompare(t, err, users, []User{u2, u1, u0}, "list")

	users, err = QueryDB[User](ctxbg, db).FilterGreaterEqual("Name", "zz").FilterLessEqual("Name", "zz").SortDesc("Name").List()
	tcompare(t, err, users, []User{u1}, "list")

	users, err = QueryDB[User](ctxbg, db).FilterGreaterEqual("Name", "zz").FilterLessEqual("Name", "zz").SortAsc("Name").List()
	tcompare(t, err, users, []User{u1}, "list")
}

func TestIDsLimit(t *testing.T) {
	type User struct {
		ID    int
		Name  string `bstore:"unique"`
		Text  string `bstore:"index"`
		Other string
	}

	const path = "testdata/tmp.idslimit.db"
	os.Remove(path)
	db, err := topen(t, path, nil, User{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	u0 := User{0, "a", "a", "a"}
	u1 := User{0, "b", "b", "b"}
	err = db.Insert(ctxbg, &u0, &u1)
	tcheck(t, err, "insert")

	// By ID.
	users, err := QueryDB[User](ctxbg, db).FilterIDs([]int{u0.ID, u1.ID}).Limit(1).List()
	tcompare(t, err, users, []User{u0}, "filterids with limit")

	users, err = QueryDB[User](ctxbg, db).FilterIDs([]int{u0.ID, u1.ID}).Limit(1).SortDesc("Name").List()
	tcompare(t, err, users, []User{u1}, "filterids with limit with sort")

	users, err = QueryDB[User](ctxbg, db).FilterIDs([]int{u0.ID, u1.ID}).Limit(1).SortDesc("ID").List()
	tcompare(t, err, users, []User{u1}, "filterids with limit with sort")

	users, err = QueryDB[User](ctxbg, db).FilterIDs([]int{u0.ID, u1.ID}).Limit(1).SortAsc("ID").List()
	tcompare(t, err, users, []User{u0}, "filterids with limit with sort")

	// By Name, unique index.
	users, err = QueryDB[User](ctxbg, db).FilterEqual("Name", "a", "b").Limit(1).List()
	tcompare(t, err, users, []User{u0}, "filterids with limit")

	users, err = QueryDB[User](ctxbg, db).FilterEqual("Name", "a", "b").Limit(1).SortDesc("Name").List()
	tcompare(t, err, users, []User{u1}, "filterids with limit with sort")

	users, err = QueryDB[User](ctxbg, db).FilterEqual("Name", "a", "b").Limit(1).SortDesc("ID").List()
	tcompare(t, err, users, []User{u1}, "filterids with limit with sort")

	users, err = QueryDB[User](ctxbg, db).FilterEqual("Name", "a", "b").Limit(1).SortAsc("ID").List()
	tcompare(t, err, users, []User{u0}, "filterids with limit with sort")

	// By Text, regular index.
	users, err = QueryDB[User](ctxbg, db).FilterEqual("Text", "a", "b").Limit(1).List()
	tcompare(t, err, users, []User{u0}, "filterids with limit")

	users, err = QueryDB[User](ctxbg, db).FilterEqual("Text", "a", "b").Limit(1).SortDesc("Text").List()
	tcompare(t, err, users, []User{u1}, "filterids with limit with sort")

	users, err = QueryDB[User](ctxbg, db).FilterEqual("Text", "a", "b").Limit(1).SortDesc("ID").List()
	tcompare(t, err, users, []User{u1}, "filterids with limit with sort")

	users, err = QueryDB[User](ctxbg, db).FilterEqual("Text", "a", "b").Limit(1).SortAsc("ID").List()
	tcompare(t, err, users, []User{u0}, "filterids with limit with sort")

	// By Other, no index.
	users, err = QueryDB[User](ctxbg, db).FilterEqual("Other", "a", "b").Limit(1).List()
	tcompare(t, err, users, []User{u0}, "filterids with limit")

	users, err = QueryDB[User](ctxbg, db).FilterEqual("Other", "a", "b").Limit(1).SortDesc("Other").List()
	tcompare(t, err, users, []User{u1}, "filterids with limit with sort")

	users, err = QueryDB[User](ctxbg, db).FilterEqual("Other", "a", "b").Limit(1).SortDesc("ID").List()
	tcompare(t, err, users, []User{u1}, "filterids with limit with sort")

	users, err = QueryDB[User](ctxbg, db).FilterEqual("Other", "a", "b").Limit(1).SortAsc("ID").List()
	tcompare(t, err, users, []User{u0}, "filterids with limit with sort")
}

// Bug: not repositioning cursor after changing (keys in a) bucket can cause
// undefined behaviour. In practice, it looked like some keys would be skipped,
// causing Delete and/or Update operations to only do partial work in certain
// situations. The new approach is gathering all records first, then delete
// them. Same for updates.
func TestDelete(t *testing.T) {
	type T struct {
		ID int64
		A  string
	}
	// These values were reliably triggering the bug when removing "b". Without an "a"
	// record that was removed first, the bug would not be triggered.
	values := []T{
		{0, "a"},
		{0, "b"},
		{0, "b"}, // This record, and every other record with "b" as value, would not be removed.
		{0, "b"},
		{0, "b"}, // Would also not be removed.
	}

	const path = "testdata/tmp.delete.db"
	os.Remove(path)
	db, err := topen(t, path, nil, T{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	for _, v := range values {
		err := db.Insert(ctxbg, &v)
		tcheck(t, err, "insert")
	}

	err = db.Write(ctxbg, func(tx *Tx) error {
		// Order does matter when removing. When removing "b" first, it will be successful.
		// Only when we remove "a" first will we trigger the bug.
		for _, dom := range []string{"a", "b"} {
			l, err := QueryTx[T](tx).FilterNonzero(T{A: dom}).List()
			tcheck(t, err, "list before")

			q := QueryTx[T](tx)
			q.FilterNonzero(T{A: dom})
			n, err := q.Delete()
			tcompare(t, err, n, len(l), "delete "+dom)

			l, err = QueryTx[T](tx).FilterNonzero(T{A: dom}).List()
			tcheck(t, err, "list after")
			tcompare(t, err, len(l), 0, "list after delete "+dom)
		}

		return nil
	})
	tcheck(t, err, "write")
}

// We must reseek a bolt cursor after making changes to the bucket we are seeking
// over. We make changes with a cursor held during Query.Delete, Query.Update*,
// and callers can do it during Query.ForEach. This test triggers various
// (corner) cases that we need to handle properly.
func TestCursorReseek(t *testing.T) {
	type T struct {
		ID int64
		S  string `bstore:"index"`
	}

	runDB := func(l []string, expectReseek uint, fn func(db *DB)) {
		t.Helper()

		// See TestDelete for this testdata.
		values := []T{
			{0, l[0]},
			{0, l[1]},
			{0, l[1]},
			{0, l[1]},
			{0, l[1]},
		}

		const path = "testdata/tmp.cursorreseek.db"
		os.Remove(path)
		db, err := topen(t, path, nil, T{})
		tcheck(t, err, "open")
		defer tclose(t, db)

		err = db.Write(ctxbg, func(tx *Tx) error {
			for _, v := range values {
				err := tx.Insert(&v)
				tcheck(t, err, "insert")
			}
			return nil
		})
		tcheck(t, err, "db write")

		fn(db)
	}

	runTx := func(l []string, expectReseek uint, fn func(tx *Tx)) {
		t.Helper()

		runDB(l, expectReseek, func(db *DB) {
			var reseek uint
			err := db.Write(ctxbg, func(tx *Tx) error {
				t.Helper()
				stats := tx.Stats()
				fn(tx)
				reseek = tx.Stats().Sub(stats).Reseek
				return nil
			})
			tcheck(t, err, "write")
			tcompare(t, nil, reseek, expectReseek, "reseek")
		})
	}

	// Insert 1 a, then 4 b, then update "a" to "c" and then "b" to "c".
	runTx([]string{"a", "b"}, 5, func(tx *Tx) {
		for _, dom := range []string{"a", "b"} {
			l, err := QueryTx[T](tx).FilterNonzero(T{S: dom}).List()
			tcheck(t, err, "list before")

			q := QueryTx[T](tx)
			q.FilterNonzero(T{S: dom})
			n, err := q.UpdateNonzero(T{S: "c"})
			tcompare(t, err, n, len(l), "update "+dom)

			l, err = QueryTx[T](tx).FilterNonzero(T{S: dom}).List()
			tcheck(t, err, "list after")
			tcompare(t, err, len(l), len(l), "list after update "+dom)
		}
	})

	// Like before, but switch order of updating to "c", first the "b" then the "a"
	runTx([]string{"a", "b"}, 5, func(tx *Tx) {
		for _, dom := range []string{"b", "a"} {
			l, err := QueryTx[T](tx).FilterNonzero(T{S: dom}).List()
			tcheck(t, err, "list before")

			q := QueryTx[T](tx)
			q.FilterNonzero(T{S: dom})
			n, err := q.UpdateNonzero(T{S: "c"})
			tcompare(t, err, n, len(l), "update "+dom)

			l, err = QueryTx[T](tx).FilterNonzero(T{S: dom}).List()
			tcheck(t, err, "list after")
			tcompare(t, err, len(l), len(l), "list after update "+dom)
		}
	})

	// Like previous two, but with 1 "c" and 1 "b" and changing them to "a".
	runTx([]string{"c", "b"}, 5, func(tx *Tx) {
		for _, dom := range []string{"c", "b"} {
			l, err := QueryTx[T](tx).FilterNonzero(T{S: dom}).List()
			tcheck(t, err, "list before")

			q := QueryTx[T](tx)
			q.FilterNonzero(T{S: dom})
			q.SortDesc("S", "ID")
			n, err := q.UpdateNonzero(T{S: "a"})
			tcompare(t, err, n, len(l), "update "+dom)

			l, err = QueryTx[T](tx).FilterNonzero(T{S: dom}).List()
			tcheck(t, err, "list after")
			tcompare(t, err, len(l), len(l), "list after update "+dom)
		}
	})

	runTx([]string{"c", "b"}, 5, func(tx *Tx) {
		for _, dom := range []string{"b", "c"} {
			l, err := QueryTx[T](tx).FilterNonzero(T{S: dom}).List()
			tcheck(t, err, "list before")

			q := QueryTx[T](tx)
			q.FilterNonzero(T{S: dom})
			q.SortDesc("S", "ID")
			n, err := q.UpdateNonzero(T{S: "a"})
			tcompare(t, err, n, len(l), "update "+dom)

			l, err = QueryTx[T](tx).FilterNonzero(T{S: dom}).List()
			tcheck(t, err, "list after")
			tcompare(t, err, len(l), len(l), "list after update "+dom)
		}
	})

	// Query.Delete.
	runTx([]string{"a", "b"}, 5, func(tx *Tx) {
		for _, dom := range []string{"a", "b"} {
			l, err := QueryTx[T](tx).FilterNonzero(T{S: dom}).List()
			tcheck(t, err, "list before")

			q := QueryTx[T](tx)
			q.FilterNonzero(T{S: dom})
			n, err := q.Delete()
			tcompare(t, err, n, len(l), "update "+dom)

			l, err = QueryTx[T](tx).FilterNonzero(T{S: dom}).List()
			tcheck(t, err, "list after")
			tcompare(t, err, len(l), 0, "list after delete "+dom)
		}
	})

	runTx([]string{"c", "b"}, 5, func(tx *Tx) {
		for _, dom := range []string{"c", "b"} {
			l, err := QueryTx[T](tx).FilterNonzero(T{S: dom}).List()
			tcheck(t, err, "list before")

			q := QueryTx[T](tx)
			q.FilterNonzero(T{S: dom})
			q.SortDesc("S", "ID")
			n, err := q.Delete()
			tcompare(t, err, n, len(l), "update "+dom)

			l, err = QueryTx[T](tx).FilterNonzero(T{S: dom}).List()
			tcheck(t, err, "list after")
			tcompare(t, err, len(l), 0, "list after delete "+dom)
		}
	})

	// With tx.Update.
	runTx([]string{"a", "b"}, 5, func(tx *Tx) {
		for _, dom := range []string{"a", "b"} {
			l, err := QueryTx[T](tx).FilterNonzero(T{S: dom}).List()
			tcheck(t, err, "list before")

			var n int
			q := QueryTx[T](tx)
			q.FilterNonzero(T{S: dom})
			err = q.ForEach(func(v T) error {
				n++
				v.S = "c"
				return tx.Update(&v)
			})
			tcompare(t, err, n, len(l), "update "+dom)

			l, err = QueryTx[T](tx).FilterNonzero(T{S: dom}).List()
			tcheck(t, err, "list after")
			tcompare(t, err, len(l), 0, "list after update "+dom)
		}
	})

	runTx([]string{"c", "b"}, 5, func(tx *Tx) {
		for _, dom := range []string{"c", "b"} {
			l, err := QueryTx[T](tx).FilterNonzero(T{S: dom}).List()
			tcheck(t, err, "list before")

			var n int
			q := QueryTx[T](tx)
			q.FilterNonzero(T{S: dom})
			q.SortDesc("S", "ID")
			err = q.ForEach(func(v T) error {
				n++
				v.S = "a"
				return tx.Update(&v)
			})
			tcompare(t, err, n, len(l), "update "+dom)

			l, err = QueryTx[T](tx).FilterNonzero(T{S: dom}).List()
			tcheck(t, err, "list after")
			tcompare(t, err, len(l), 0, "list after update "+dom)
		}
	})

	// With tx.Delete.
	runTx([]string{"a", "b"}, 5, func(tx *Tx) {
		for _, dom := range []string{"a", "b"} {
			l, err := QueryTx[T](tx).FilterNonzero(T{S: dom}).List()
			tcheck(t, err, "list before")

			var n int
			q := QueryTx[T](tx)
			q.FilterNonzero(T{S: dom})
			err = q.ForEach(func(v T) error {
				n++
				return tx.Delete(&v)
			})
			tcompare(t, err, n, len(l), "update "+dom)

			l, err = QueryTx[T](tx).FilterNonzero(T{S: dom}).List()
			tcheck(t, err, "list after")
			tcompare(t, err, len(l), 0, "list after update "+dom)
		}
	})

	runTx([]string{"c", "b"}, 5, func(tx *Tx) {
		for _, dom := range []string{"c", "b"} {
			l, err := QueryTx[T](tx).FilterNonzero(T{S: dom}).List()
			tcheck(t, err, "list before")

			var n int
			q := QueryTx[T](tx)
			q.FilterNonzero(T{S: dom})
			q.SortDesc("S", "ID")
			err = q.ForEach(func(v T) error {
				n++
				return tx.Delete(&v)
			})
			tcompare(t, err, n, len(l), "update "+dom)

			l, err = QueryTx[T](tx).FilterNonzero(T{S: dom}).List()
			tcheck(t, err, "list after")
			tcompare(t, err, len(l), 0, "list after update "+dom)
		}
	})

	// With tx.Insert.
	runTx([]string{"a", "b"}, 5, func(tx *Tx) {
		for _, dom := range []string{"a", "b"} {
			l, err := QueryTx[T](tx).FilterNonzero(T{S: dom}).List()
			tcheck(t, err, "list before")

			var n int
			q := QueryTx[T](tx)
			q.FilterNonzero(T{S: dom})
			err = q.ForEach(func(v T) error {
				if err := tx.Insert(&T{0, "Z"}); err != nil {
					return err
				}
				if err := tx.Insert(&T{0, "c"}); err != nil {
					return err
				}
				n++
				return nil
			})
			tcompare(t, err, n, len(l), "update "+dom)

			nl, err := QueryTx[T](tx).FilterNonzero(T{S: dom}).List()
			tcheck(t, err, "list after")
			tcompare(t, err, len(nl), len(l), "list after update "+dom)
		}
	})

	// Using QueryDB.
	runDB([]string{"a", "b"}, 5, func(db *DB) {
		for _, dom := range []string{"a", "b"} {
			l, err := QueryDB[T](ctxbg, db).FilterNonzero(T{S: dom}).List()
			tcheck(t, err, "list before")

			n, err := QueryDB[T](ctxbg, db).FilterNonzero(T{S: dom}).SortDesc("S", "ID").Delete()
			tcompare(t, err, n, len(l), "delete "+dom)

			l, err = QueryDB[T](ctxbg, db).FilterNonzero(T{S: dom}).List()
			tcompare(t, err, len(l), 0, "list after delete "+dom)
		}
	})
}

// TestCursorReseekStowed tests the code path for "stowed" keys (stowed because of
// a sort that uses a partial index and collects matching values).
func TestCursorReseekStowed(t *testing.T) {
	type T struct {
		ID    int64
		S     string `bstore:"index"`
		Other string
	}

	t.Helper()

	// Similar to data from TestDelete.
	values := []T{
		{0, "a", "E"},
		{0, "b", "D"},
		{0, "b", "C"},
		{0, "b", "B"},
		{0, "b", "A"},
		{0, "c", "@"},
	}

	const path = "testdata/tmp.cursorreseekstowed.db"
	os.Remove(path)
	db, err := topen(t, path, nil, T{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	err = db.Write(ctxbg, func(tx *Tx) error {
		for _, v := range values {
			err := tx.Insert(&v)
			tcheck(t, err, "insert")
		}
		return nil
	})
	tcheck(t, err, "db write")

	stats := db.Stats()
	err = db.Write(ctxbg, func(tx *Tx) error {
		for _, dom := range []string{"a", "b"} {
			l, err := QueryTx[T](tx).SortAsc("S", "Other").List()
			tcheck(t, err, "list before")

			n, err := QueryTx[T](tx).SortAsc("S", "Other").Delete()
			tcompare(t, err, n, len(l), "delete "+dom)

			l, err = QueryTx[T](tx).List()
			tcompare(t, err, len(l), 0, "list after delete "+dom)
		}
		return nil
	})
	tcheck(t, err, "db write")
	reseek := db.Stats().Sub(stats).Reseek
	// Once for each index key prefix for "S".
	tcompare(t, nil, reseek, uint(3), "reseek")

	// Now in reverse.
	err = db.Write(ctxbg, func(tx *Tx) error {
		for _, v := range values {
			err := tx.Insert(&v)
			tcheck(t, err, "insert")
		}
		return nil
	})
	tcheck(t, err, "db write")
	stats = db.Stats()
	err = db.Write(ctxbg, func(tx *Tx) error {
		for _, dom := range []string{"a", "b"} {
			l, err := QueryTx[T](tx).SortDesc("S", "Other").List()
			tcheck(t, err, "list before")

			n, err := QueryTx[T](tx).SortDesc("S", "Other").Delete()
			tcompare(t, err, n, len(l), "delete "+dom)

			l, err = QueryTx[T](tx).List()
			tcompare(t, err, len(l), 0, "list after delete "+dom)
		}
		return nil
	})
	tcheck(t, err, "db write")
	reseek = db.Stats().Sub(stats).Reseek
	// Once for each index key prefix for "S".
	tcompare(t, nil, reseek, uint(3), "reseek")

	// Again in reverse, but we delete all other records in between collection data
	// pairs in exec, to force a seek to the end.
	err = db.Write(ctxbg, func(tx *Tx) error {
		for _, v := range values {
			err := tx.Insert(&v)
			tcheck(t, err, "insert")
		}
		return nil
	})
	tcheck(t, err, "db write")
	stats = db.Stats()
	err = db.Write(ctxbg, func(tx *Tx) error {
		var n int
		err := QueryTx[T](tx).SortDesc("S", "Other").ForEach(func(v T) error {
			if n == 0 {
				_, err := QueryTx[T](tx).Delete()
				tcheck(t, err, "delete all")
			}
			n++
			return nil
		})
		// 1 "c", the others are removed.
		tcompare(t, err, n, 1, "foreach while deleting")

		l, err := QueryTx[T](tx).List()
		tcompare(t, err, len(l), 0, "list after delete")
		return nil
	})
	tcheck(t, err, "db write")
	reseek = db.Stats().Sub(stats).Reseek
	// 1 for ForEach, 6 for each row deleted.
	tcompare(t, nil, reseek, uint(1+6), "reseek")

	// Similar with reverse order and deleting while iterating, causing move to a key we already collected.
	err = db.Write(ctxbg, func(tx *Tx) error {
		for _, v := range values {
			err := tx.Insert(&v)
			tcheck(t, err, "insert")
		}
		return nil
	})
	tcheck(t, err, "db write")
	stats = db.Stats()
	err = db.Write(ctxbg, func(tx *Tx) error {
		var n int
		err := QueryTx[T](tx).FilterLess("S", "c").SortDesc("S", "Other").ForEach(func(v T) error {
			if n == 0 {
				_, err := QueryTx[T](tx).FilterNonzero(T{S: "a"}).Delete()
				tcheck(t, err, "delete a")
			}
			n++
			return nil
		})
		// 4 "b" that were collected before removing.
		tcompare(t, err, n, 4, "foreach while deleting")

		l, err := QueryTx[T](tx).List()
		tcompare(t, err, len(l), 4+1, "list after delete")
		return nil
	})
	tcheck(t, err, "db write")
	reseek = db.Stats().Sub(stats).Reseek
	// 1 for Delete during ForEach, 1 for deleted "a"
	tcompare(t, nil, reseek, uint(1+1), "reseek")
}

func TestSortIndex(t *testing.T) {
	type T struct {
		ID   int64
		Time time.Time `bstore:"index"`
		A    string
	}

	const path = "testdata/tmp.sortindex.db"
	os.Remove(path)
	db, err := topen(t, path, nil, T{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	now := time.Now().Round(0)
	values := []T{
		{0, now.Add(0 * time.Second), "a"},
		{0, now.Add(0 * time.Second), "b"},
		{0, now.Add(1 * time.Second), "c"},
		{0, now.Add(1 * time.Second), "d"},
		{0, now.Add(-1 * time.Second), "e"},
		{0, now.Add(-1 * time.Second), "f"},
	}
	for i := range values {
		err := db.Insert(ctxbg, &values[i])
		tcheck(t, err, "insert")
	}

	var stats, delta Stats
	stats = db.Stats()

	updateStats := func() {
		nstats := db.Stats()
		delta = nstats.Sub(stats)
		stats = nstats
	}

	l, err := QueryDB[T](ctxbg, db).SortAsc("Time", "A").List()
	tcheck(t, err, "query")
	exp := []T{values[4], values[5], values[0], values[1], values[2], values[3]}
	tcompare(t, err, l, exp, "sort asc by time,id")

	l, err = QueryDB[T](ctxbg, db).SortAsc("Time", "A").Limit(3).List()
	tcheck(t, err, "query")
	tcompare(t, err, l, exp[:3], "sort asc by time,id with limit")

	l, err = QueryDB[T](ctxbg, db).SortDesc("Time", "A").List()
	tcheck(t, err, "query")
	exp = []T{values[3], values[2], values[1], values[0], values[5], values[4]}
	tcompare(t, err, l, exp, "sort desc by time,id")

	l, err = QueryDB[T](ctxbg, db).SortDesc("Time", "A").Limit(3).List()
	tcheck(t, err, "query")
	tcompare(t, err, l, exp[:3], "sort desc by time,id with limit")

	l, err = QueryDB[T](ctxbg, db).SortAsc("Time").SortDesc("A").List()
	tcheck(t, err, "query")
	exp = []T{values[5], values[4], values[1], values[0], values[3], values[2]}
	tcompare(t, err, l, exp, "sort by asc time, desc id")

	l, err = QueryDB[T](ctxbg, db).SortAsc("Time").SortDesc("A").Limit(3).List()
	tcheck(t, err, "query")
	tcompare(t, err, l, exp[:3], "sort by asc time, desc id")

	l, err = QueryDB[T](ctxbg, db).SortDesc("Time").SortAsc("A").List()
	tcheck(t, err, "query")
	exp = []T{values[2], values[3], values[0], values[1], values[4], values[5]}
	tcompare(t, err, l, exp, "sort by desc time, asc id")

	l, err = QueryDB[T](ctxbg, db).SortDesc("Time").SortAsc("A").Limit(3).List()
	tcheck(t, err, "query")
	tcompare(t, err, l, exp[:3], "sort by desc time, asc id with limit")

	updateStats()
	tcompare(t, nil, delta.Sort, uint(8), "sort used by all queries")
}

func TestSortIndexString(t *testing.T) {
	type T struct {
		ID int64
		S  string `bstore:"index"`
	}

	const path = "testdata/tmp.sortindexstring.db"
	os.Remove(path)
	db, err := topen(t, path, nil, T{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	values := []T{
		{0, "aa"},
		{0, "aa"},
		{0, "b"},
		{0, "b"},
		{0, "a"},
		{0, "a"},
	}
	for i := range values {
		err := db.Insert(ctxbg, &values[i])
		tcheck(t, err, "insert")
	}

	var stats, delta Stats
	stats = db.Stats()

	updateStats := func() {
		nstats := db.Stats()
		delta = nstats.Sub(stats)
		stats = nstats
	}

	l, err := QueryDB[T](ctxbg, db).SortAsc("S", "ID").List()
	tcheck(t, err, "query")
	exp := []T{values[4], values[5], values[0], values[1], values[2], values[3]}
	tcompare(t, err, l, exp, "sort asc by str,id")

	l, err = QueryDB[T](ctxbg, db).SortAsc("S", "ID").Limit(3).List()
	tcheck(t, err, "query")
	tcompare(t, err, l, exp[:3], "sort asc by str,id with limit")

	l, err = QueryDB[T](ctxbg, db).SortDesc("S", "ID").List()
	tcheck(t, err, "query")
	exp = []T{values[3], values[2], values[1], values[0], values[5], values[4]}
	tcompare(t, err, l, exp, "sort desc by str,id")

	l, err = QueryDB[T](ctxbg, db).SortDesc("S", "ID").Limit(3).List()
	tcheck(t, err, "query")
	tcompare(t, err, l, exp[:3], "sort desc by str,id with limit")

	updateStats()
	tcompare(t, nil, delta.Sort, uint(0), "4 queries sorted using only index")

	l, err = QueryDB[T](ctxbg, db).SortAsc("S").SortDesc("ID").List()
	tcheck(t, err, "query")
	exp = []T{values[5], values[4], values[1], values[0], values[3], values[2]}
	tcompare(t, err, l, exp, "sort by asc str, desc id")

	l, err = QueryDB[T](ctxbg, db).SortAsc("S").SortDesc("ID").Limit(3).List()
	tcheck(t, err, "query")
	tcompare(t, err, l, exp[:3], "sort by asc str, desc id")

	l, err = QueryDB[T](ctxbg, db).SortDesc("S").SortAsc("ID").List()
	tcheck(t, err, "query")
	exp = []T{values[2], values[3], values[0], values[1], values[4], values[5]}
	tcompare(t, err, l, exp, "sort by desc str, asc id")

	l, err = QueryDB[T](ctxbg, db).SortDesc("S").SortAsc("ID").Limit(3).List()
	tcheck(t, err, "query")
	tcompare(t, err, l, exp[:3], "sort by desc str, asc id with limit")

	updateStats()
	tcompare(t, nil, delta.Sort, uint(4), "4 queries sorted in memory")
}

func TestSortIndexMultiple(t *testing.T) {
	type T struct {
		ID int64
		A  string `bstore:"index A+B"`
		B  string
	}

	const path = "testdata/tmp.sortindexmultiple.db"
	os.Remove(path)
	db, err := topen(t, path, nil, T{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	values := []T{
		{0, "aa", "x"},
		{0, "aa", "z"},
		{0, "aa", "y"},
		{0, "aa", "x"},
		{0, "b", "x"},
		{0, "a", "x"},
		{0, "a", "x"},
	}
	for i := range values {
		err := db.Insert(ctxbg, &values[i])
		tcheck(t, err, "insert")
	}

	var stats, delta Stats
	stats = db.Stats()

	updateStats := func() {
		nstats := db.Stats()
		delta = nstats.Sub(stats)
		stats = nstats
	}

	// Use full index ascending.
	l, err := QueryDB[T](ctxbg, db).SortAsc("A", "B", "ID").List()
	tcheck(t, err, "query")
	exp := []T{values[5], values[6], values[0], values[3], values[2], values[1], values[4]}
	tcompare(t, err, l, exp, "sort asc by a,b,id")
	updateStats()
	tcompare(t, nil, delta.Sort, uint(0), "in-memory sort")
	tcompare(t, nil, delta.PlanIndexScan, uint(1), "index scan")

	// Use full index descending.
	slices.Reverse(exp)
	l, err = QueryDB[T](ctxbg, db).SortDesc("A", "B", "ID").List()
	tcheck(t, err, "query")
	tcompare(t, err, l, exp, "sort desc by a,b,id")
	updateStats()
	tcompare(t, nil, delta.Sort, uint(0), "in-memory sort")
	tcompare(t, nil, delta.PlanIndexScan, uint(1), "index scan")

	// Use index scan for A and in-memory sort for B,ID.
	l, err = QueryDB[T](ctxbg, db).SortAsc("A").SortDesc("B").SortAsc("ID").List()
	tcheck(t, err, "query")
	exp = []T{values[5], values[6], values[1], values[2], values[0], values[3], values[4]}
	tcompare(t, err, l, exp, "sort a asc, b desc, id asc")
	updateStats()
	tcompare(t, nil, delta.Sort, uint(1), "in memory sort")
	tcompare(t, nil, delta.PlanIndexScan, uint(1), "index scan")

	// Again, with limit.
	l, err = QueryDB[T](ctxbg, db).SortAsc("A").SortDesc("B").SortAsc("ID").Limit(4).List()
	tcheck(t, err, "query")
	tcompare(t, err, l, exp[:4], "sort a asc, b desc, id asc")
	updateStats()

	// Use index scan for A,B and in-memory sort for ID.
	l, err = QueryDB[T](ctxbg, db).SortAsc("A", "B").SortDesc("ID").List()
	tcheck(t, err, "query")
	exp = []T{values[6], values[5], values[3], values[0], values[2], values[1], values[4]}
	tcompare(t, err, l, exp, "sort a asc, b desc, id desc")
	updateStats()
	tcompare(t, nil, delta.Sort, uint(1), "in memory sort")
	tcompare(t, nil, delta.PlanIndexScan, uint(1), "index scan")

	// Again, with limit.
	l, err = QueryDB[T](ctxbg, db).SortAsc("A", "B").SortDesc("ID").Limit(4).List()
	tcheck(t, err, "query")
	tcompare(t, err, l, exp[:4], "sort a asc, b desc, id desc")
	updateStats()

	// Use index with filtering on A and sorting by B,ID.
	l, err = QueryDB[T](ctxbg, db).FilterEqual("A", "aa").SortAsc("B", "ID").List()
	tcheck(t, err, "query")
	exp = []T{values[0], values[3], values[2], values[1]}
	tcompare(t, err, l, exp, "filter a=aa, sort b,id asc")
	updateStats()
	tcompare(t, nil, delta.Sort, uint(0), "in memory sort")
	tcompare(t, nil, delta.PlanIndexScan, uint(1), "index scan")

	// Still use index with filtering on A and sorting by A,B,ID (A ordering ignored).
	l, err = QueryDB[T](ctxbg, db).FilterEqual("A", "aa").SortAsc("A", "B", "ID").List()
	tcheck(t, err, "query")
	exp = []T{values[0], values[3], values[2], values[1]}
	tcompare(t, err, l, exp, "filter a=aa, sort a,b,id asc")
	updateStats()
	tcompare(t, nil, delta.Sort, uint(0), "in memory sort")
	tcompare(t, nil, delta.PlanIndexScan, uint(1), "index scan")

	// Use both index and in-memory sorting for A=aa and B asc, ID desc.
	l, err = QueryDB[T](ctxbg, db).FilterEqual("A", "aa").SortAsc("B").SortDesc("ID").List()
	tcheck(t, err, "query")
	exp = []T{values[3], values[0], values[2], values[1]}
	tcompare(t, err, l, exp, "filter a=aa, sort b asc,id desc")
	updateStats()
	tcompare(t, nil, delta.Sort, uint(1), "in memory sort")
	tcompare(t, nil, delta.PlanIndexScan, uint(1), "index scan")

	// xxx also with filter and with sort on same field

	// Use both index and in-memory sorting for A=aa and B desc, ID asc.
	l, err = QueryDB[T](ctxbg, db).FilterEqual("A", "aa").SortDesc("B").SortAsc("ID").List()
	tcheck(t, err, "query")
	exp = []T{values[1], values[2], values[0], values[3]}
	tcompare(t, err, l, exp, "filter a=aa, sort b desc,id asc")
	updateStats()
	tcompare(t, nil, delta.Sort, uint(1), "in memory sort")
	tcompare(t, nil, delta.PlanIndexScan, uint(1), "index scan")

	// Use index with compare on A and sorting by A,B,ID.
	l, err = QueryDB[T](ctxbg, db).FilterGreaterEqual("A", "aa").SortAsc("A", "B", "ID").List()
	tcheck(t, err, "query")
	exp = []T{values[0], values[3], values[2], values[1], values[4]}
	tcompare(t, err, l, exp, "filter a>=aa, sort a,b,id asc")
	updateStats()
	tcompare(t, nil, delta.Sort, uint(0), "memory sort")
	tcompare(t, nil, delta.PlanIndexScan, uint(1), "index scan")

	// Use index with compare on A and sorting by A,B,ID desc.
	slices.Reverse(exp)
	l, err = QueryDB[T](ctxbg, db).FilterGreaterEqual("A", "aa").SortDesc("A", "B", "ID").List()
	tcheck(t, err, "query")
	tcompare(t, err, l, exp, "filter a>=aa, sort a,b,id desc")
	updateStats()
	tcompare(t, nil, delta.Sort, uint(0), "memory sort")
	tcompare(t, nil, delta.PlanIndexScan, uint(1), "index scan")

	// Use index with compare on A and in-memory sort for sorting by B,ID.
	l, err = QueryDB[T](ctxbg, db).FilterGreaterEqual("A", "aa").SortAsc("B", "ID").List()
	tcheck(t, err, "query")
	exp = []T{values[0], values[3], values[4], values[2], values[1]}
	tcompare(t, err, l, exp, "filter a>=aa, sort b,id asc")
	updateStats()
	tcompare(t, nil, delta.Sort, uint(1), "in-memory sort")
	tcompare(t, nil, delta.PlanIndexScan, uint(1), "index scan")
}

func TestSortIndexInsliceMulti(t *testing.T) {
	type T struct {
		ID int64
		A  []string `bstore:"index A+B"`
		B  string
	}

	const path = "testdata/tmp.sortindexinslicemulti.db"
	os.Remove(path)
	db, err := topen(t, path, nil, T{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	values := []T{
		{0, []string{"aa"}, "x"},
		{0, []string{"aa"}, "z"},
		{0, []string{"aa"}, "y"},
		{0, []string{"aa"}, "x"},
		{0, []string{"b"}, "x"},
		{0, []string{"a"}, "x"},
		{0, []string{"a"}, "x"},
	}
	for i := range values {
		err := db.Insert(ctxbg, &values[i])
		tcheck(t, err, "insert")
	}

	var stats, delta Stats
	stats = db.Stats()

	updateStats := func() {
		nstats := db.Stats()
		delta = nstats.Sub(stats)
		stats = nstats
	}

	// Use both index and in-memory sorting for A=aa and B desc, ID asc.
	l, err := QueryDB[T](ctxbg, db).FilterIn("A", "aa").SortDesc("B").SortAsc("ID").List()
	tcheck(t, err, "query")
	exp := []T{values[1], values[2], values[0], values[3]}
	tcompare(t, err, l, exp, "filter a=aa, sort b desc,id asc")
	updateStats()
	tcompare(t, nil, delta.Sort, uint(1), "in memory sort")
	tcompare(t, nil, delta.PlanIndexScan, uint(1), "index scan")

	// Use only index, for both keys.
	l, err = QueryDB[T](ctxbg, db).FilterIn("A", "aa").SortDesc("B", "ID").List()
	tcheck(t, err, "query")
	exp = []T{values[1], values[2], values[3], values[0]}
	tcompare(t, err, l, exp, "filter a=aa, sort b desc,id desc")
	updateStats()
	tcompare(t, nil, delta.Sort, uint(0), "in memory sort")
	tcompare(t, nil, delta.PlanIndexScan, uint(1), "index scan")
}

func TestSortIndexPK(t *testing.T) {
	type T struct {
		ID int64
		A  string `bstore:"index"`
	}

	const path = "testdata/tmp.sortindexpk.db"
	os.Remove(path)
	db, err := topen(t, path, nil, T{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	values := []T{
		{0, "aa"},
		{0, "aa"},
		{0, "aa"},
		{0, "aa"},
		{0, "b"},
		{0, "a"},
		{0, "a"},
	}
	for i := range values {
		err := db.Insert(ctxbg, &values[i])
		tcheck(t, err, "insert")
	}

	var stats, delta Stats
	stats = db.Stats()

	updateStats := func() {
		nstats := db.Stats()
		delta = nstats.Sub(stats)
		stats = nstats
	}

	// Filter on equal by A, and use ID for index ordering.
	l, err := QueryDB[T](ctxbg, db).FilterEqual("A", "aa").SortAsc("ID").List()
	tcheck(t, err, "query")
	exp := []T{values[0], values[1], values[2], values[3]}
	tcompare(t, err, l, exp, "filter a=aa, id asc")
	updateStats()
	tcompare(t, nil, delta.Sort, uint(0), "in memory sort")
	tcompare(t, nil, delta.PlanIndexScan, uint(1), "index scan")

	// Filter on equal by A, and use A,ID for index ordering.
	l, err = QueryDB[T](ctxbg, db).FilterEqual("A", "aa").SortAsc("A", "ID").List()
	tcheck(t, err, "query")
	exp = []T{values[0], values[1], values[2], values[3]}
	tcompare(t, err, l, exp, "filter a=aa, sort a asc, id asc")
	updateStats()
	tcompare(t, nil, delta.Sort, uint(0), "in memory sort")
	tcompare(t, nil, delta.PlanIndexScan, uint(1), "index scan")

	// Filter on equal by A, and use A,ID desc for index ordering.
	l, err = QueryDB[T](ctxbg, db).FilterEqual("A", "aa").SortDesc("A", "ID").List()
	tcheck(t, err, "query")
	exp = []T{values[3], values[2], values[1], values[0]}
	tcompare(t, err, l, exp, "filter a=aa, sort a desc, id desc")
	updateStats()
	tcompare(t, nil, delta.Sort, uint(0), "in memory sort")
	tcompare(t, nil, delta.PlanIndexScan, uint(1), "index scan")
}

func TestSortIndexPKStr(t *testing.T) {
	type T struct {
		PK string
		A  string `bstore:"index"`
	}

	const path = "testdata/tmp.sortindexpk.db"
	os.Remove(path)
	db, err := topen(t, path, nil, T{})
	tcheck(t, err, "open")
	defer tclose(t, db)

	values := []T{
		{"l", "aa"},
		{"ll", "aa"},
		{"lll", "aa"},
		{"r", "aa"},
		{"s", "b"},
		{"t", "a"},
		{"u", "a"},
	}
	for i := range values {
		err := db.Insert(ctxbg, &values[i])
		tcheck(t, err, "insert")
	}

	var stats, delta Stats
	stats = db.Stats()

	updateStats := func() {
		nstats := db.Stats()
		delta = nstats.Sub(stats)
		stats = nstats
	}

	// Filter on equal by A, and use PK for index ordering.
	l, err := QueryDB[T](ctxbg, db).FilterEqual("A", "aa").SortAsc("PK").List()
	tcheck(t, err, "query")
	exp := []T{values[0], values[1], values[2], values[3]}
	tcompare(t, err, l, exp, "filter a=aa, pk asc")
	updateStats()
	tcompare(t, nil, delta.Sort, uint(0), "in memory sort")
	tcompare(t, nil, delta.PlanIndexScan, uint(1), "index scan")

	// Filter on equal by A, and use A,PK for index ordering.
	l, err = QueryDB[T](ctxbg, db).FilterEqual("A", "aa").SortAsc("A", "PK").List()
	tcheck(t, err, "query")
	exp = []T{values[0], values[1], values[2], values[3]}
	tcompare(t, err, l, exp, "filter a=aa, sort a asc, pk asc")
	updateStats()
	tcompare(t, nil, delta.Sort, uint(0), "in memory sort")
	tcompare(t, nil, delta.PlanIndexScan, uint(1), "index scan")

	// Filter on equal by A, and use A,PK desc for index ordering.
	l, err = QueryDB[T](ctxbg, db).FilterEqual("A", "aa").SortDesc("A", "PK").List()
	tcheck(t, err, "query")
	exp = []T{values[3], values[2], values[1], values[0]}
	tcompare(t, err, l, exp, "filter a=aa, sort a desc, pk desc")
	updateStats()
	tcompare(t, nil, delta.Sort, uint(0), "in memory sort")
	tcompare(t, nil, delta.PlanIndexScan, uint(1), "index scan")

	// Filter on equal by A, and use A asc,PK desc for index ordering.
	l, err = QueryDB[T](ctxbg, db).FilterEqual("A", "aa").SortAsc("A").SortDesc("PK").List()
	tcheck(t, err, "query")
	exp = []T{values[3], values[2], values[1], values[0]}
	tcompare(t, err, l, exp, "filter a=aa, sort a asc, pk desc")
	updateStats()
	tcompare(t, nil, delta.Sort, uint(0), "in memory sort")
	tcompare(t, nil, delta.PlanIndexScan, uint(1), "index scan")
}

package bstore_test

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	"github.com/mjl-/bstore"
)

func Example() {
	// Msg and Mailbox are the types we are going to store.
	type Msg struct {
		// First field is always primary key (PK) and must be non-zero.
		// Integer types get their IDs assigned from a sequence when
		// inserted with the zero value.
		ID uint64

		// MailboxID must be nonzero, it references the PK of Mailbox
		// (enforced), a combination MailboxID+UID must be unique
		// (enforced) and we create an additional index on
		// MailboxID+Received for faster queries.
		MailboxID uint32 `bstore:"nonzero,ref Mailbox,unique MailboxID+UID,index MailboxID+Received"`

		// UID must be nonzero, for IMAP.
		UID uint32 `bstore:"nonzero"`

		// Received is nonzero too, and also gets its own index.
		Received time.Time `bstore:"nonzero,index"`

		From string
		To   string
		Seen bool
		Data []byte
		// ... an actual mailbox message would have more fields...
	}

	type Mailbox struct {
		ID   uint32
		Name string `bstore:"unique"`
	}

	// For tests.
	os.Mkdir("testdata", 0700)
	const path = "testdata/mail.db"
	os.Remove(path)

	ctx := context.Background() // Possibly replace with a request context.

	// Open or create database mail.db, and register types Msg and Mailbox.
	// Bstore automatically creates (unique) indices.
	// If you had previously opened this database with types of the same
	// name but not the exact field types, bstore checks the types are
	// compatible and makes any changes necessary, such as
	// creating/replacing indices, verifying new constraints (unique,
	// nonzero, references).
	db, err := bstore.Open(ctx, path, nil, Msg{}, Mailbox{})
	if err != nil {
		log.Fatalln("open:", err)
	}
	defer db.Close()

	// Insert mailboxes. Because the primary key is zero, the next
	// autoincrement/sequence is assigned to the ID field.
	var (
		inbox   = Mailbox{Name: "INBOX"}
		sent    = Mailbox{Name: "Sent"}
		archive = Mailbox{Name: "Archive"}
		trash   = Mailbox{Name: "Trash"}
	)
	if err := db.Insert(ctx, &inbox, &sent, &archive, &trash); err != nil {
		log.Fatalln("insert mailbox:", err)
	}

	// Insert messages, IDs are automatically assigned.
	now := time.Now()
	var (
		msg0 = Msg{MailboxID: inbox.ID, UID: 1, Received: now.Add(-time.Hour)}
		msg1 = Msg{MailboxID: inbox.ID, UID: 2, Received: now.Add(-time.Second), Seen: true}
		msg2 = Msg{MailboxID: inbox.ID, UID: 3, Received: now}
		msg3 = Msg{MailboxID: inbox.ID, UID: 4, Received: now.Add(-time.Minute)}
		msg4 = Msg{MailboxID: trash.ID, UID: 1, Received: now}
		msg5 = Msg{MailboxID: trash.ID, UID: 2, Received: now}
		msg6 = Msg{MailboxID: archive.ID, UID: 1, Received: now}
	)
	if err := db.Insert(ctx, &msg0, &msg1, &msg2, &msg3, &msg4, &msg5, &msg6); err != nil {
		log.Fatalln("insert messages:", err)
	}

	// Get a single record by ID using Get.
	nmsg0 := Msg{ID: msg0.ID}
	if err := db.Get(ctx, &nmsg0); err != nil {
		log.Fatalln("get:", err)
	}

	// ErrAbsent is returned if the record does not exist.
	if err := db.Get(ctx, &Msg{ID: msg0.ID + 999}); err != bstore.ErrAbsent {
		log.Fatalln("get did not return ErrAbsent:", err)
	}

	// Inserting duplicate values results in ErrUnique.
	if err := db.Insert(ctx, &Msg{MailboxID: trash.ID, UID: 1, Received: now}); err == nil || !errors.Is(err, bstore.ErrUnique) {
		log.Fatalln("inserting duplicate message did not return ErrUnique:", err)
	}

	// Inserting fields that reference non-existing records results in ErrReference.
	if err := db.Insert(ctx, &Msg{MailboxID: trash.ID + 999, UID: 1, Received: now}); err == nil || !errors.Is(err, bstore.ErrReference) {
		log.Fatalln("inserting reference to absent mailbox did not return ErrReference:", err)
	}

	// Deleting records that are still referenced results in ErrReference.
	if err := db.Delete(ctx, &Mailbox{ID: inbox.ID}); err == nil || !errors.Is(err, bstore.ErrReference) {
		log.Fatalln("deleting mailbox that is still referenced did not return ErrReference:", err)
	}

	// Updating a record checks constraints.
	nmsg0 = msg0
	nmsg0.UID = 2 // Not unique.
	if err := db.Update(ctx, &nmsg0); err == nil || !errors.Is(err, bstore.ErrUnique) {
		log.Fatalln("updating message to already present UID did not return ErrUnique:", err)
	}

	nmsg0 = msg0
	nmsg0.Received = time.Time{} // Zero value.
	if err := db.Update(ctx, &nmsg0); err == nil || !errors.Is(err, bstore.ErrZero) {
		log.Fatalln("updating message to zero Received did not return ErrZero:", err)
	}

	// Use a transaction with DB.Write or DB.Read for a consistent view.
	err = db.Write(ctx, func(tx *bstore.Tx) error {
		// tx also has Insert, Update, Delete, Get.
		// But we can compose and execute proper queries.
		//
		// We can call several Filter* and Sort* methods that all add
		// to the query. We end with an operation like Count, Get (a
		// single record), List (all selected records), Delete (delete
		// selected records), Update, etc.
		//
		// FilterNonzero filters on the nonzero field values of its
		// parameter.  Since "false" is a zero value, we cannot use
		// FilterNonzero but use FilterEqual instead.  We also want the
		// messages in "newest first" order.
		//
		// QueryTx and QueryDB must be called on the package, because
		// type parameters cannot be introduced on methods in Go.
		q := bstore.QueryTx[Msg](tx)
		q.FilterNonzero(Msg{MailboxID: inbox.ID})
		q.FilterEqual("Seen", false)
		q.SortDesc("Received")
		msgs, err := q.List()
		if err != nil {
			log.Fatalln("listing unseen inbox messages, newest first:", err)
		}
		if len(msgs) != 3 || msgs[0].ID != msg2.ID || msgs[1].ID != msg3.ID || msgs[2].ID != msg0.ID {
			log.Fatalf("listing unseen inbox messages, got %v, expected message ids %d,%d,%d", msgs, msg2.ID, msg3.ID, msg0.ID)
		}

		// The index on MailboxID,Received was used automatically to
		// retrieve the messages efficiently in sorted order without
		// requiring a fetch + in-memory sort.
		stats := tx.Stats()
		if stats.PlanIndexScan != 1 {
			log.Fatalf("index scan was not used (%d)", stats.PlanIndexScan)
		} else if stats.Sort != 0 {
			log.Fatalf("in-memory sort was performed (%d)", stats.Sort)
		}

		// We can use filters to select records to delete.
		// Note the chaining: filters return the same, modified query.
		// Operations like Delete finish the query. Don't put too many
		// filters in a single chained statement, for readability.
		n, err := bstore.QueryTx[Msg](tx).FilterNonzero(Msg{MailboxID: trash.ID}).Delete()
		if err != nil {
			log.Fatalln("deleting messages from trash:", err)
		} else if n != 2 {
			log.Fatalf("deleted %d messages from trash, expected 2", n)
		}

		// We can select messages to update, e.g. to mark all messages in inbox as seen.
		// We can also gather the records or their IDs that are removed, similar to SQL "returning".
		var updated []Msg
		q = bstore.QueryTx[Msg](tx)
		q.FilterNonzero(Msg{MailboxID: inbox.ID})
		q.FilterEqual("Seen", false)
		q.SortDesc("Received")
		q.Gather(&updated)
		n, err = q.UpdateNonzero(Msg{Seen: true})
		if err != nil {
			log.Fatalln("update messages in inbox to seen:", err)
		} else if n != 3 || len(updated) != 3 {
			log.Fatalf("updated %d messages %v, expected 3", n, updated)
		}

		// We can also iterate over the messages one by one. Below we
		// iterate over just the IDs efficiently, use .Next() for
		// iterating over the full messages.
		stats = tx.Stats()
		var ids []uint64
		q = bstore.QueryTx[Msg](tx).FilterNonzero(Msg{MailboxID: inbox.ID}).SortAsc("Received")
		for {
			var id uint64
			if err := q.NextID(&id); err == bstore.ErrAbsent {
				// No more messages.
				// Note: if we don't iterate until an error, Close must be called on the query for cleanup.
				break
			} else if err != nil {
				log.Fatalln("iterating over IDs:", err)
			}
			// The ID is fetched from the index. The full record is
			// never read from the database. Calling Next instead
			// of NextID does always fetch, parse and return the
			// full record.
			ids = append(ids, id)
		}
		if len(ids) != 4 || ids[0] != msg0.ID || ids[1] != msg3.ID || ids[2] != msg1.ID || ids[3] != msg2.ID {
			log.Fatalf("iterating over IDs, got %v, expected %d,%d,%d,%d", ids, msg0.ID, msg3.ID, msg1.ID, msg2.ID)
		}
		delta := tx.Stats().Sub(stats)
		if delta.Index.Cursor == 0 || delta.Records.Get != 0 {
			log.Fatalf("no index was scanned (%d), or records were fetched (%d)", delta.Index.Cursor, delta.Records.Get)
		}

		// Return success causing transaction to commit.
		return nil
	})
	if err != nil {
		log.Fatalln("write transaction:", err)
	}

	// Output:
}

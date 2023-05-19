package epos

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexSimple(t *testing.T) {
	that := assert.New(t)

	// Prepare
	db, err := OpenDatabase("testdb_index", STORAGE_AUTO)
	if err != nil {
		panic(fmt.Sprintf("couldn't open testdb_index: %v", err))
	}
	defer db.Close()

	// Execute: Insert
	firstId, err := db.Coll("tbl").Insert(map[string]string{"foo": "bar", "baz": "quux", "bla": "fasel"})
	that.Nil(err)
	that.Greater(firstId, Id(0))

	// Execute: AddIndex
	err = db.Coll("tbl").AddIndex("foo")
	that.Nil(err)

	// Execute: Insert again
	secondId, err := db.Coll("tbl").Insert(map[string]string{"foo": "abc", "baz": "def", "bla": "asdfqwer"})
	that.Nil(err)
	that.Greater(secondId, firstId)

	// Validate internal data structures
	that.NotNil(db.Coll("tbl").indexes["foo"])
	that.Len(db.Coll("tbl").indexes["foo"].data, 2)

	// Clean up
	db.Remove()
}

func TestReadWriteIndex(t *testing.T) {
	that := assert.New(t)

	// Prepare
	buf := bytes.NewBuffer([]byte{})
	idx_written := &indexEntry{deleted: false, value: "value", id: 9001}

	// Execute
	bytesWritten, err := idx_written.WriteTo(buf)
	that.Nil(err)

	idx_read := &indexEntry{}
	bytesRead, err := idx_read.ReadFrom(buf)
	that.Nil(err)

	// Evaluate
	that.Equal(bytesWritten, bytesRead)
	that.EqualValues(idx_written, idx_read)
}

type entry struct {
	X string
	Y int
	Z float64
}

var testdata = []struct {
	Entry entry
	NewX  string
	NewY  int
	NewZ  float64
	Id    Id
}{
	{Entry: entry{X: "John Doe", Y: 23, Z: 1.85}, NewX: "Max Mustermann", NewY: 42, NewZ: 1.83, Id: Id(0)},
	{Entry: entry{X: "Jan Maier", Y: 17, Z: 1.75}, NewX: "Franz Huber", NewY: 19, NewZ: 1.97, Id: Id(0)},
	{Entry: entry{X: "Franz Haber", Y: 19, Z: 1.90}, NewX: "Franz Haber-Oettinger", NewY: 19, NewZ: 1.90, Id: Id(0)},
}

func TestIndexInsertUpdateDelete(t *testing.T) {

	that := assert.New(t)

	// Prepare
	db, err := OpenDatabase("testdb_index_iud", STORAGE_AUTO)
	if err != nil {
		panic(fmt.Sprintf("couldn't open testdb_index_iud: %v", err))
	}
	defer db.Close()

	coll := db.Coll("persons")
	coll.AddIndex("X")

	compareId := Id(0)
	for i, e := range testdata {
		id, err := coll.Insert(e.Entry)
		that.Nil(err)               // Insert worked OK
		that.Greater(id, compareId) // ID incremented

		testdata[i].Id = id
		testdata[i].Entry.X = e.NewX
		testdata[i].Entry.Y = e.NewY
		testdata[i].Entry.Z = e.NewZ

		compareId = id
	}

	// Verify that the index was created correctly
	that.Len(coll.indexes["X"].data, 3)

	// Verify that adding an index to an existing data set works
	coll.AddIndex("Y")
	that.Len(coll.indexes["Y"].data, 3)

	// Verify that the updates work
	for _, e := range testdata {
		err := coll.Update(e.Id, e.Entry)
		that.Nil(err)
	}

	// Verify that the indexes are good
	that.Len(coll.indexes["X"].data, 3)
	that.Len(coll.indexes["Y"].data, 2)       // Two records ("19" and "42")
	that.Len(coll.indexes["Y"].data["19"], 2) // Two records for index "19"

	for _, e := range testdata {
		found := false
		for k, v := range coll.indexes["X"].data {
			if k == e.Entry.X && len(v) == 1 && v[0].id == int64(e.Id) {
				found = true
				break
			}
		}
		that.True(found, "couldn't find entry in X index for data: ", e.Entry.X)
	}

	for _, e := range testdata {
		err := coll.Delete(e.Id)
		that.Nil(err)
	}

	that.Len(coll.indexes["X"].data, 0)
	that.Len(coll.indexes["Y"].data, 0)

	err = db.Vacuum()
	that.Nil(err)

	// Clean up
	db.Remove()
}

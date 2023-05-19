package epos

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStore(t *testing.T) {

	that := assert.New(t)

	// Prepare
	db, err := OpenDatabase("testdb1", STORAGE_AUTO)
	if err != nil {
		panic(fmt.Sprintf("couldn't open testdb1: %v", err))
	}
	defer db.Close()

	// Execute 1
	id, err := db.Coll("foo").Insert([]string{"hello", "world!"})
	that.Nil(err, "slice")
	that.Equal(id, Id(1), "slice")

	// Execute 2
	id, err = db.Coll("foo").Insert(struct{ X, Y string }{X: "pan-galactic", Y: "gargle-blaster"})
	that.Nil(err, "struct")
	that.Equal(id, Id(2), "struct")

	// Execute 3 & clean up
	err = db.Remove()
	that.Nil(err)

}

var benchmarkData = struct {
	Name         string
	Age          uint
	SSN          string
	LuckyNumbers []int
}{
	Name:         "John J. McWhackadoodle",
	Age:          29,
	SSN:          "078-05-1120",
	LuckyNumbers: []int{23, 43},
}

func BenchmarkInsertDiskv(b *testing.B) {
	benchmarkInsert(b, STORAGE_DISKV)
}

func benchmarkInsert(b *testing.B, typ StorageType) {
	b.StopTimer()

	db, _ := OpenDatabase(fmt.Sprintf("testdb_bench_insert_%s", typ), typ)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, err := db.Coll("bench").Insert(benchmarkData)
		if err != nil {
			b.Fatal("insert failed: ", err)
		}
	}

	b.StopTimer()
	db.Close()
	db.Remove()
}

func BenchmarkUpdateDiskv(b *testing.B) {
	benchmarkUpdate(b, STORAGE_DISKV)
}

func benchmarkUpdate(b *testing.B, typ StorageType) {
	b.StopTimer()

	db, _ := OpenDatabase(fmt.Sprintf("testdb_bench_update_%s", typ), typ)

	id, err := db.Coll("bench").Insert(benchmarkData)
	if err != nil {
		b.Fatal("insert failed: ", err)
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		benchmarkData.LuckyNumbers[0], benchmarkData.LuckyNumbers[1] = benchmarkData.LuckyNumbers[1], benchmarkData.LuckyNumbers[0]
		if err = db.Coll("bench").Update(id, benchmarkData); err != nil {
			b.Fatal("update failed: ", err)
		}
	}

	b.StopTimer()
	db.Close()
	db.Remove()
}

func BenchmarkDeleteDiskv(b *testing.B) {
	benchmarkDelete(b, STORAGE_DISKV)
}

func benchmarkDelete(b *testing.B, typ StorageType) {
	b.StopTimer()

	db, _ := OpenDatabase(fmt.Sprintf("testdb_bench_delete_%s", typ), typ)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		id, err := db.Coll("bench").Insert(benchmarkData)
		if err != nil {
			b.Fatal("insert failed: ", err)
		}
		b.StartTimer()
		if err = db.Coll("bench").Delete(id); err != nil {
			b.Fatal("delete failed: ", err)
		}
	}

	b.StopTimer()
	db.Close()
	db.Remove()
}

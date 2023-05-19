package epos

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type book struct {
	Title  string
	Author string
	Price  float64
	Pages  int
}

var queryData = []book{
	{"Fables", "Aesop", 17.95, 239},
	{"Adventures of Huckleberry Finn", "Mark Twain", 7.95, 364},
	{"Alice's Adventures in Wonderland", "Lewis Caroll", 12.45, 375},
	{"Cinderella", "George Routledge & Sons", 8.95, 145},
	{"Dracula", "Bram Stoker", 23.95, 729},
	{"The Jungle Book", "Rudyard Kipling", 10.95, 396},
	{"Tom Sawyer Aboard", "Mark Twain", 9.99, 270},
}

func TestQueries(t *testing.T) {

	that := assert.New(t)

	// Prepare
	db, err := OpenDatabase("testdb_queries", STORAGE_AUTO)
	if err != nil {
		t.Fatalf("couldn't open testdb_queries: %v", err)
	}
	defer db.Close()

	books := db.Coll("books")

	books.AddIndex("Author")

	for i, book := range queryData {
		_, err := books.Insert(book)
		if err != nil {
			t.Errorf("%d. Insert failed: %v", i, err)
		}
	}

	// Execute: Query on ID
	var b1 book
	queryById, err := books.QueryId(1)
	that.Nil(err)

	that.True(queryById.Next(nil, &b1))
	that.Equal("Fables", b1.Title)
	that.False(queryById.Next(nil, &b1))

	// Execute: Query on ID
	var b2 book
	var id Id
	queryByAuthor, err := books.Query(&Equals{Field: "Author", Value: "Mark Twain"})
	that.Nil(err)
	countByAuthor := 0
	for queryByAuthor.Next(&id, &b2) {
		that.Equal("Mark Twain", b2.Author)
		countByAuthor++
	}
	that.Equal(2, countByAuthor)

	// Prepare index
	books.AddIndex("Pages")

	// Execute
	var b3 book
	queryUsingIndex, err := books.Query(&Or{&Equals{Field: "Author", Value: "Aesop"}, &Equals{Field: "Pages", Value: "270"}})
	that.Nil(err)
	countByIndex := 0
	for queryUsingIndex.Next(nil, &b3) {
		that.Contains([]int{270, 239}, b3.Pages)
		that.Contains([]string{"Aesop", "Mark Twain"}, b3.Author)
		countByIndex++
	}
	that.Equal(2, countByIndex)

	_, err = books.Query(&Equals{Field: "Name", Value: "Fables"})
	that.NotNil(err)

	// Clean up
	db.Remove()
}

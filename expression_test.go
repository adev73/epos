package epos

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExpressionParser(t *testing.T) {

	that := assert.New(t)

	testdata := []struct {
		Expr       string
		ShouldFail bool
	}{
		{"(id 1)", false},
		{"(foobar)", true},
		{"(or (id 23) (id 42))", false},
		{"(eq id_str 3738888)", false},
		{"(eq)", true},
		{"(id)", true},
		{"(eq foo)", true},
		{"(or)", true},
		{"(and)", true},
	}

	for _, tt := range testdata {
		_, err := Expression(tt.Expr)
		if tt.ShouldFail {
			that.NotNil(err, "parsing expression succeded when it should have failed", tt.Expr)
		} else {
			that.Nil(err, "parsing expression failed when it should have succeeded", tt.Expr)
		}
	}
}

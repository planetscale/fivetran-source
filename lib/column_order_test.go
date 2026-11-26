package lib

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumnOrderChange(t *testing.T) {
	saved := []string{"id", "a", "b", "c"}
	for _, tc := range []struct {
		name    string
		saved   []string
		current []string
		want    string
	}{
		{"identical", saved, []string{"id", "a", "b", "c"}, ""},
		{"identical, different case", saved, []string{"ID", "A", "b", "C"}, ""},
		{"append at end", saved, []string{"id", "a", "b", "c", "newcol"}, ""},
		{"append two at end", saved, []string{"id", "a", "b", "c", "x", "y"}, ""},
		{"no saved order", nil, []string{"newcol", "id", "a", "b", "c"}, ""},
		{"no current order", saved, nil, ""},
		{"drop only", saved, []string{"id", "a", "c"}, ""},
		{"drop last only", saved, []string{"id", "a", "b"}, ""},
		{"insert mid (ADD COLUMN newcol AFTER a)", saved, []string{"id", "a", "newcol", "b", "c"}, "new column newcol was added before existing column b"},
		{"insert first (ADD COLUMN newcol FIRST)", saved, []string{"newcol", "id", "a", "b", "c"}, "new column newcol was added before existing column id"},
		{"insert mid and append", saved, []string{"id", "x", "a", "b", "c", "y"}, "new column x was added before existing column a"},
		{"reorder (MODIFY c AFTER a)", saved, []string{"id", "a", "c", "b"}, "column b moved before column c"},
		{"reorder first (MODIFY c FIRST)", saved, []string{"c", "id", "a", "b"}, "column id moved before column c"},
		{"drop and re-add at end (positions shift)", saved, []string{"id", "a", "c", "b"}, "column b moved before column c"},
		{"drop and append (same column count)", saved, []string{"id", "a", "c", "d"}, "column(s) b dropped and column(s) d added"},
		{"drop and append (more columns)", saved, []string{"id", "a", "b", "x", "y"}, "column(s) c dropped and column(s) x,y added"},
		{"rename in place (drop + add mid)", saved, []string{"id", "a", "b2", "c"}, "new column b2 was added before existing column c"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, ColumnOrderChange(tc.saved, tc.current))
		})
	}
}

func TestShardStatesColumnOrderIsBackwardCompatible(t *testing.T) {
	// State written before the field existed parses with no saved order and
	// is written back unchanged.
	old := `{"shards":{"-":{"cursor":"abc"}}}`
	var ss ShardStates
	assert.NoError(t, json.Unmarshal([]byte(old), &ss))
	assert.Nil(t, ss.ColumnOrder)
	out, err := json.Marshal(ss)
	assert.NoError(t, err)
	assert.JSONEq(t, old, string(out))

	ss.ColumnOrder = []string{"id", "a"}
	out, err = json.Marshal(ss)
	assert.NoError(t, err)
	assert.JSONEq(t, `{"shards":{"-":{"cursor":"abc"}},"column_order":["id","a"]}`, string(out))
}

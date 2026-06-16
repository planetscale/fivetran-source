package lib

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestShardsForKeyspace_SiblingKeyspacePrefix covers the case where
// a database name ("test") shares a prefix with a sibling sharded keyspace
// ("test_sharded"). "show vitess_shards like \"%test%\"" returned rows for both
// keyspaces.
func TestShardsForKeyspace_SiblingKeyspacePrefix(t *testing.T) {
	rows := []string{
		"test/-",
		"test_sharded/-80",
		"test_sharded/80-",
	}

	t.Run("test_sharded returns only its own shards, prefix stripped", func(t *testing.T) {
		got := shardsForKeyspace("test_sharded", rows)

		assert.Equal(t, []string{"-80", "80-"}, got)
		// The exact bug: the sibling keyspace's shard must not leak in, and no
		// returned shard name may carry a keyspace prefix.
		assert.NotContains(t, got, "-", "the unsharded sibling keyspace's shard leaked in")
		for _, s := range got {
			assert.NotContains(t, s, "/", "shard name still has a keyspace prefix: %q", s)
		}
	})

	t.Run("test returns only its own shard, not the sibling's", func(t *testing.T) {
		got := shardsForKeyspace("test", rows)
		assert.Equal(t, []string{"-"}, got)
	})

	t.Run("unknown keyspace returns nothing", func(t *testing.T) {
		assert.Empty(t, shardsForKeyspace("other", rows))
	})

	t.Run("malformed rows without a separator are skipped", func(t *testing.T) {
		assert.Empty(t, shardsForKeyspace("test", []string{"test", ""}))
	})
}

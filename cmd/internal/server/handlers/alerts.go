package handlers

import (
	"fmt"
	"sort"
	"strings"
)

// maxAlertItems caps how many tables/shards are listed in one alert message.
const maxAlertItems = 20

// binlogExpirationSet de-duplicates "keyspace.table (shard s)" entries for the
// combined binlog-expiration Warning.
type binlogExpirationSet struct {
	seen  map[string]struct{}
	items []string
}

func (b *binlogExpirationSet) add(keyspace, table, shard string) {
	item := fmt.Sprintf("%s.%s (shard %s)", keyspace, table, shard)
	if b.seen == nil {
		b.seen = map[string]struct{}{}
	}
	if _, ok := b.seen[item]; ok {
		return
	}
	b.seen[item] = struct{}{}
	b.items = append(b.items, item)
}

func (b *binlogExpirationSet) warningMessage() string {
	if len(b.items) == 0 {
		return ""
	}
	return "PlanetScale binary logs expired before Fivetran could read the latest changes for the following table(s): " +
		joinLimited(b.items) + ". " +
		"An automatic historical re-sync has been initiated. No action is needed."
}

// StaleShardsWarningMessage builds the customer-facing Warning sent when stale
// shard entries written by an older connector version are dropped from state.
// prunedByKeyspace maps keyspace -> pruned shard names.
func StaleShardsWarningMessage(prunedByKeyspace map[string][]string) string {
	if len(prunedByKeyspace) == 0 {
		return ""
	}
	keyspaces := make([]string, 0, len(prunedByKeyspace))
	for ks := range prunedByKeyspace {
		keyspaces = append(keyspaces, ks)
	}
	sort.Strings(keyspaces)
	// The cap applies to the total number of shards listed, across keyspaces.
	var items []string
	listed, total := 0, 0
	for _, ks := range keyspaces {
		shards := append([]string(nil), prunedByKeyspace[ks]...)
		sort.Strings(shards)
		total += len(shards)
		if room := maxAlertItems - listed; room > 0 {
			if len(shards) > room {
				shards = shards[:room]
			}
			listed += len(shards)
			items = append(items, fmt.Sprintf("keyspace %s: %s", ks, strings.Join(shards, ", ")))
		}
	}
	list := strings.Join(items, ", ")
	if total > listed {
		list += fmt.Sprintf(" and %d more", total-listed)
	}
	return "PlanetScale removed stale shard references left in this connection's sync state by an older connector version (" +
		list + "). " +
		"These references never contained data. No action or re-sync is needed."
}

func joinLimited(items []string) string {
	if len(items) <= maxAlertItems {
		return strings.Join(items, ", ")
	}
	return strings.Join(items[:maxAlertItems], ", ") + fmt.Sprintf(" and %d more", len(items)-maxAlertItems)
}

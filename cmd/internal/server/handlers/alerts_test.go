package handlers

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBinlogExpirationWarningListIsCapped(t *testing.T) {
	var b binlogExpirationSet
	for i := 0; i < maxAlertItems+5; i++ {
		b.add("ks", fmt.Sprintf("t%02d", i), "-")
	}
	msg := b.warningMessage()
	assert.Equal(t, maxAlertItems, strings.Count(msg, "(shard -)"))
	assert.Contains(t, msg, " and 5 more. ")
}

func TestStaleShardsWarningMessage(t *testing.T) {
	assert.Empty(t, StaleShardsWarningMessage(nil))
	assert.Equal(t,
		"PlanetScale removed stale shard references left in this connection's sync state by an older connector version "+
			"(keyspace a: a_sharded/-80, keyspace b: b_sharded/-). These references never contained data. No action or re-sync is needed.",
		StaleShardsWarningMessage(map[string][]string{"b": {"b_sharded/-"}, "a": {"a_sharded/-80"}}))
}

func TestStaleShardsWarningCapsTotalShards(t *testing.T) {
	var shards []string
	for i := 0; i < maxAlertItems+5; i++ {
		shards = append(shards, fmt.Sprintf("s%02d", i))
	}
	msg := StaleShardsWarningMessage(map[string][]string{"ks1": shards})
	assert.Equal(t, maxAlertItems, strings.Count(msg, ", s")+1)
	assert.Contains(t, msg, "(keyspace ks1: s00, s01, ")
	assert.Contains(t, msg, "s19 and 5 more). ")
	assert.NotContains(t, msg, "s20")
}

func TestStaleShardsWarningCapSpansKeyspaces(t *testing.T) {
	var a, b []string
	for i := 0; i < 15; i++ {
		a = append(a, fmt.Sprintf("a%02d", i))
		b = append(b, fmt.Sprintf("b%02d", i))
	}
	msg := StaleShardsWarningMessage(map[string][]string{"b": b, "a": a})
	assert.Contains(t, msg, "keyspace a: a00, ")
	assert.Contains(t, msg, "a14, keyspace b: b00, ")
	assert.Contains(t, msg, "b04 and 10 more). ")
	assert.NotContains(t, msg, "b05")
}

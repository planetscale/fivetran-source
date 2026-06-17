//go:build integration

package lib

import "time"

func SetCursorCheckpointPolicyForIntegrationTest(rows int, interval time.Duration) func() {
	previousRows := cursorCheckpointRows
	previousInterval := cursorCheckpointInterval

	cursorCheckpointRows = rows
	cursorCheckpointInterval = interval

	return func() {
		cursorCheckpointRows = previousRows
		cursorCheckpointInterval = previousInterval
	}
}

package main

import (
	"bufio"
	"fmt"

	nomad "github.com/hashicorp/nomad/api"
)

const (
	maxLineSize = 32 * 1024
)

// getLocalNodeID returns the node ID of the local Nomad Client and an error if
// it couldn't be determined or the Agent is not running in Client mode.
func getLocalNodeID(client *nomad.Client) (string, error) {
	info, err := client.Agent().Self()
	if err != nil {
		return "", fmt.Errorf("Error querying agent info: %s", err)
	}
	clientStats, ok := info.Stats["client"]
	if !ok {
		return "", fmt.Errorf("Nomad not running in client mode")
	}

	nodeID, ok := clientStats["node_id"]
	if !ok {
		return "", fmt.Errorf("Failed to determine node ID")
	}

	return nodeID, nil
}

func isTerminal(alloc *nomad.Allocation) bool {
	switch alloc.ClientStatus {
	case nomad.AllocClientStatusComplete, nomad.AllocClientStatusFailed, nomad.AllocClientStatusLost:
		return true
	default:
		return false
	}
}

// sizeSpliter wraps a bufio.SplitFunc by limiting the max size of the split.
// This allow us not to use too much memory and avoid the bufio.ErrTooLong.

// This works by checking the output of the SplitFunc. If no token is found
// and the provided buffer length is greater than maxSize it will be returned
// as a new token. If a SplitFunc finds a token earlier, that token will be
// returned.
func sizeSpliter(maxSize int, splitFunc bufio.SplitFunc) bufio.SplitFunc {
	return func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		advance, token, err = splitFunc(data, atEOF)
		if err != nil {
			return
		}
		if advance == 0 && token == nil && len(data) > maxSize {
			advance = len(data)
			token = data
		}
		return
	}
}

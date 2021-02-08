package cachetype

import (
	"fmt"

	"github.com/hashicorp/consul/agent/cache"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/go-hclog"
)

// Recommended name for registration.
const IntentionMatchName = "intention-match"

// IntentionMatch supports fetching the intentions via match queries.
type IntentionMatch struct {
	RegisterOptionsBlockingRefresh
	RPC RPC
}

var logger = hclog.Default()

func (c *IntentionMatch) Fetch(opts cache.FetchOptions, req cache.Request) (cache.FetchResult, error) {
	var result cache.FetchResult

	// The request should be an IntentionQueryRequest.
	reqReal, ok := req.(*structs.IntentionQueryRequest)
	if !ok {
		return result, fmt.Errorf(
			"Internal cache failure: request wrong type: %T", req)
	}

	// Lightweight copy this object so that manipulating QueryOptions doesn't race.
	dup := *reqReal
	reqReal = &dup

	// Set the minimum query index to our current index so we block
	reqReal.MinQueryIndex = opts.MinIndex
	reqReal.MaxQueryTime = opts.Timeout

	// Fetch
	var reply structs.IndexedIntentionMatches
	if err := c.RPC.RPC("Intention.Match", reqReal, &reply); err != nil {
		return result, err
	}

	// If there is a change in intention, increment the counter
	if reply.Index != reqReal.MinQueryIndex {
		// Get service name from the request
		if reqReal.Match != nil {
			serviceEntries := reqReal.Match.Entries
			if len(serviceEntries) > 0 {
				logger.Info("intention match", "service name", serviceEntries[0].Name, "datacenter", reqReal.Datacenter)
			}
		}
	}

	result.Value = &reply
	result.Index = reply.Index
	return result, nil
}

package main

import (
	"context"
	"log"
	"sync"
	"time"

	nomad "github.com/hashicorp/nomad/api"
	"github.com/jorgemarey/nomad-log-shipper/output"
	"github.com/jorgemarey/nomad-log-shipper/storage"
)

// Agent defines a log recolection process
type Agent struct {
	NodeID string
	dc     string
	client Client

	// a map with all allocation information
	tracked    map[string]*Collector
	trackMtx   sync.Mutex
	shutdownCh chan struct{}

	outputs map[string]output.Output
	store   storage.Store
}

// NewAgent creates a log recollection agent
func NewAgent(nodeID, datacenter string, client Client, outs map[string]output.Output, store storage.Store) (*Agent, error) {
	return &Agent{
		NodeID:     nodeID,
		dc:         datacenter,
		client:     client,
		tracked:    make(map[string]*Collector),
		shutdownCh: make(chan struct{}),
		outputs:    outs,
		store:      store,
	}, nil
}

// Run boots up the agent to begin log recolecction
func (a *Agent) Run() {
	a.store.Initialize()

	opts := &nomad.QueryOptions{WaitTime: 5 * time.Minute} // TODO: set context to cancel
	for {
		allocs, meta, err := a.client.Allocations(a.NodeID, opts)
		if err != nil {
			// TODO: log error
			time.Sleep(5 * time.Second)
			continue
		}

		select {
		case <-a.shutdownCh:
			return
		default:
		}

		// If no change in index we just cycle again
		if opts.WaitIndex == meta.LastIndex {
			continue
		}

		for _, alloc := range allocs {
			// If the allocation hasn't changed do nothing
			if opts.WaitIndex >= alloc.ModifyIndex {
				continue
			}
			go a.modifiedAllocation(alloc)
		}
		opts.WaitIndex = meta.LastIndex
	}
}

// Shutdown tells the agent to stop collecting logs
func (a *Agent) Shutdown(ctx context.Context) error {
	a.trackMtx.Lock()
	defer a.trackMtx.Unlock()
	close(a.shutdownCh)

	// stop every collector
	finishCh := make(chan struct{})
	for _, c := range a.tracked {
		go func(c *Collector) {
			c.Stop()
			finishCh <- struct{}{}
		}(c)
	}
	for range a.tracked {
		<-finishCh
	}
	// TODO: wait for processing to finish
	for _, out := range a.outputs {
		out.Close()
	}
	a.store.Close()
	return nil
}

func (a *Agent) modifiedAllocation(alloc *nomad.Allocation) {
	terminal := isTerminal(alloc)
	a.trackMtx.Lock()
	defer a.trackMtx.Unlock()
	collector, ok := a.tracked[alloc.ID]

	switch {
	// Allocation is in terminal state but it's still tracked. Stop and remove
	case terminal && ok:
		collector.Shutdown()
		delete(a.tracked, alloc.ID)
		log.Printf("Finished recollection for alloc: %s", alloc.ID)
	// Allocation is already removed and terminal
	case terminal:
		return
	case ok:
		collector.Update(alloc)
		return
	default:
		collector = NewAllocCollector(alloc, a.dc, a.client, a.outputs, a.store.AllocationStorer((alloc.ID)))
		if collector.Start() {
			a.tracked[alloc.ID] = collector
			log.Printf("Start recollection for alloc: %s", alloc.ID)
		}
	}
}

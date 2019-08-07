package main

import (
	"context"
	"sync"
	"time"

	nomad "github.com/hashicorp/nomad/api"
	"github.com/jorgemarey/nomad-log-shipper/output"
	"github.com/jorgemarey/nomad-log-shipper/storage"
)

// Agent defines a log recolection process
type Agent struct {
	NodeID string
	client Client

	// here will appear all allocation logs
	logCh chan *output.LogFrame
	// a map with all allocation information
	tracked    map[string]*Collector
	trackMtx   sync.Mutex
	shutdownCh chan struct{}

	output output.Output
	store  storage.Store
}

// NewAgent creates a log recollection agent
func NewAgent(nodeID string, client Client, out output.Output, store storage.Store) (*Agent, error) {
	return &Agent{
		NodeID:     nodeID,
		client:     client,
		logCh:      make(chan *output.LogFrame),
		tracked:    make(map[string]*Collector),
		shutdownCh: make(chan struct{}),
		output:     out,
		store:      store,
	}, nil
}

// Run boots up the agent to begin log recolecction
func (a *Agent) Run() {
	a.store.Initialize()
	go a.process() // TODO: boot this up knowingly (add an stop or cancel chan to stop this before ending)

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
	// Allocation is already removed and terminal
	case terminal:
		return
	case ok:
		collector.Update(alloc)
		return
	default:
		collector = NewAllocCollector(alloc, a.client, a.logCh, a.store.AllocationStorer((alloc.ID)))
		if collector.Start() {
			a.tracked[alloc.ID] = collector
		}
	}
}

// TESTING
func (a *Agent) process() {
	for {
		select {
		case log := <-a.logCh:
			log.Meta["version"] = version
			a.output.Write(log)
		}
	}
}

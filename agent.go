package main

import (
	"time"

	nomad "github.com/hashicorp/nomad/api"
	"github.com/jorgemarey/nomad-log-shipper/output"
)

// Agent defines a log recolection process
type Agent struct {
	NodeID string
	client Client

	// here will appear all allocation logs
	logCh chan *output.LogFrame
	// a map with all allocation information
	tracked map[string]*Collector
	output  output.Output
}

// NewAgent creates a log recollection agent
func NewAgent(nodeID string, client Client, out output.Output) (*Agent, error) {
	return &Agent{
		NodeID:  nodeID,
		client:  client,
		logCh:   make(chan *output.LogFrame),
		tracked: make(map[string]*Collector),
		output:  out,
	}, nil
}

// Start boots up the agent to begin log recolecction
func (a *Agent) Start() {
	go a.process() // TODO: boot this up knowingly (add an stop or cancel chan to stop this before ending)

	opts := &nomad.QueryOptions{WaitTime: 5 * time.Minute}
	// TODO: set context to cancel
	for {
		allocs, meta, err := a.client.Allocations(a.NodeID, opts)
		if err != nil {
			// TODO: log error
			time.Sleep(5 * time.Second)
			continue
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
			a.modifiedAllocation(alloc)
		}
		opts.WaitIndex = meta.LastIndex
	}
}

// Stop tells the agent to stop collecting logs
func (a *Agent) Stop() error {
	// TODO: call every collector stop and wait for them
	// TODO: wait for processing to finish
	return nil
}

func (a *Agent) modifiedAllocation(alloc *nomad.Allocation) {
	terminal := isTerminal(alloc)
	collector, ok := a.tracked[alloc.ID]

	switch {
	// Allocation is in terminal state but it's still tracked. Stop and remove
	case terminal && ok:
		collector.Shutdown() // TODO: here we wait to finish, we should send this in background
		delete(a.tracked, alloc.ID)
	// Allocation is already removed and terminal
	case terminal:
		return
	case ok:
		collector.Update(alloc)
		return
	default:
		collector = NewAllocCollector(alloc, a.client, a.logCh)
		a.tracked[alloc.ID] = collector
		go collector.Start()
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

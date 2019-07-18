package main

import nomad "github.com/hashicorp/nomad/api"

// LogGetter defines an interface to a nomad log recollection object
type LogGetter interface {
	Logs(alloc *nomad.Allocation, follow bool, task, logType, origin string,
		offset int64, cancel <-chan struct{}, q *nomad.QueryOptions) (<-chan *nomad.StreamFrame, <-chan error)
}

// Client is an interface to nomad client with the usefull functions
type Client interface {
	Allocations(nodeID string, q *nomad.QueryOptions) ([]*nomad.Allocation, *nomad.QueryMeta, error)
	LogGetter
}

type apiClient struct {
	client *nomad.Client
}

func (c *apiClient) Allocations(nodeID string, q *nomad.QueryOptions) ([]*nomad.Allocation, *nomad.QueryMeta, error) {
	return c.client.Nodes().Allocations(nodeID, q)
}

func (c *apiClient) Logs(alloc *nomad.Allocation, follow bool, task, logType, origin string,
	offset int64, cancel <-chan struct{}, q *nomad.QueryOptions) (<-chan *nomad.StreamFrame, <-chan error) {
	return c.client.AllocFS().Logs(alloc, follow, task, logType, origin, offset, cancel, q)
}

func newClient(client *nomad.Client) Client {
	return &apiClient{client: client}
}

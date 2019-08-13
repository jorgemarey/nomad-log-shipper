package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	nomad "github.com/hashicorp/nomad/api"
	"github.com/jorgemarey/nomad-log-shipper/output"
	"github.com/jorgemarey/nomad-log-shipper/output/nats"
	"github.com/jorgemarey/nomad-log-shipper/storage/boltdb"
)

const version = "0.1.2"

func main() {
	var nodeID string
	var dc string
	flag.StringVar(&nodeID, "nodeID", "", "node to get allocations from")
	flag.StringVar(&dc, "dc", "", "datacenter used to run this allocation")
	flag.Parse()

	client, err := nomad.NewClient(nomad.DefaultConfig())
	if err != nil {
		log.Fatal(err)
	}

	if nodeID == "" {
		nodeID, err = getLocalNodeID(client)
		if err != nil {
			log.Fatal(err)
		}
	}

	if dc == "" {
		node, _, err := client.Nodes().Info(nodeID, nil)
		if err != nil {
			log.Fatal(err)
		}
		dc = node.Datacenter
	}

	store := boltdb.NewBoltDBStore()
	agent, _ := NewAgent(nodeID, dc, newClient(client), outputs(), store) // wont fail

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	go agent.Run()

	select {
	case s := <-signalCh:
		log.Printf("Captured %v. Exiting...", s)
		agent.Shutdown(context.Background())
	}
}

func outputs() map[string]output.Output {
	logOut, err := nats.NewNatsStreamingOutput("log")
	if err != nil {
		if err != nil {
			log.Fatal(err)
		}
	}
	spanOut, err := nats.NewNatsStreamingOutput("span")
	if err != nil {
		if err != nil {
			log.Fatal(err)
		}
	}

	return map[string]output.Output{
		"log":  logOut,
		"span": spanOut,
	}
}

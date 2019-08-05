package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	nomad "github.com/hashicorp/nomad/api"
	"github.com/jorgemarey/nomad-log-shipper/output/natsserver"
	"github.com/jorgemarey/nomad-log-shipper/storage/boltdb"
)

const version = "0.0.1"

func main() {
	// TODO: read flags and configuration files
	client, _ := nomad.NewClient(nomad.DefaultConfig())
	id, _ := getLocalNodeID(client)

	output, _ := natsserver.NewNatsOutput()
	store := boltdb.NewBoltDBStore()

	agent, _ := NewAgent(id, newClient(client), output, store)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	go agent.Run()

	select {
	case s := <-signalCh:
		log.Printf("Captured %v. Exiting...", s)
		agent.Shutdown(context.Background())
	}
}

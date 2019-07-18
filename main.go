package main

import (
	nomad "github.com/hashicorp/nomad/api"
	"github.com/jorgemarey/nomad-log-shipper/output/file"
)

const version = "0.0.1"

func main() {
	// TODO: flags and configuration

	client, _ := nomad.NewClient(nomad.DefaultConfig())
	id, _ := getLocalNodeID(client)

	output, _ := file.NewFileOutput("stdout")

	agent, _ := NewAgent(id, newClient(client), output)
	agent.Start()

	// TODO: wait for signal
}

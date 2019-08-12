package nats

import (
	"os"

	"github.com/jorgemarey/nomad-log-shipper/output"
	stan "github.com/nats-io/stan.go"
)

type natsStreamingOutput struct {
	sc stan.Conn
}

// NewNatsStreamingOutput return
func NewNatsStreamingOutput( /*TODO options*/ ) (output.Output, error) {
	output := &natsStreamingOutput{}
	sc, err := stan.Connect(os.Getenv("STAN_CLUSTER_ID"), os.Getenv("STAN_CLIENT_ID"), stan.NatsURL(os.Getenv("NATS_URL")))
	output.sc = sc

	return output, err
}

func (o *natsStreamingOutput) Write(p []byte) (int, error) {
	err := o.sc.Publish(os.Getenv("STAN_SUBJECT"), p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (o *natsStreamingOutput) Close() error {
	return o.sc.Close()
}

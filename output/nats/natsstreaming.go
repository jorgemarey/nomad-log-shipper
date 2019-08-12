package nats

import (
	"fmt"
	"os"

	"github.com/jorgemarey/nomad-log-shipper/output"
	stan "github.com/nats-io/stan.go"
)

type NatsStreamingOutput struct {
	sc stan.Conn

	subject string
}

// NewNatsStreamingOutput return
func NewNatsStreamingOutput(name string) (*NatsStreamingOutput, error) {
	output := &NatsStreamingOutput{subject: os.Getenv(fmt.Sprintf("STAN_SUBJECT_%s", name))}
	sc, err := stan.Connect(os.Getenv("STAN_CLUSTER_ID"), os.Getenv("STAN_CLIENT_ID"), stan.NatsURL(os.Getenv("NATS_URL")))
	if err != nil {
		return nil, err
	}
	output.sc = sc
	return output, err
}

func (o *NatsStreamingOutput) Output(subject string) output.Output {
	return &NatsStreamingOutput{sc: o.sc, subject: subject}
}

func (o *NatsStreamingOutput) Write(p []byte) (int, error) {
	err := o.sc.Publish(o.subject, p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (o *NatsStreamingOutput) Close() error {
	return o.sc.Close()
}

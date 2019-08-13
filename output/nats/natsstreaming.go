package nats

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/jorgemarey/nomad-log-shipper/output"
	stan "github.com/nats-io/stan.go"
)

type NatsStreamingOutput struct {
	sc stan.Conn

	subject string

	connP bool
	l     sync.RWMutex
}

// NewNatsStreamingOutput return
func NewNatsStreamingOutput(name string) (*NatsStreamingOutput, error) {
	output := &NatsStreamingOutput{subject: os.Getenv(fmt.Sprintf("STAN_SUBJECT_%s", name)), connP: true}
	sc, err := stan.Connect(os.Getenv("STAN_CLUSTER_ID"), os.Getenv("STAN_CLIENT_ID"),
		stan.NatsURL(os.Getenv("NATS_URL")),
		stan.SetConnectionLostHandler(output.lostHandler),
	)
	if err != nil {
		return nil, err
	}
	output.sc = sc
	return output, err
}

func (o *NatsStreamingOutput) Output(name string) output.Output {
	return &NatsStreamingOutput{sc: o.sc, subject: os.Getenv(fmt.Sprintf("STAN_SUBJECT_%s", name)), connP: true}
}

func (o *NatsStreamingOutput) Write(p []byte) (int, error) {
	for {
		if o.connStatus() {
			break
		}
		time.Sleep(1 * time.Second)
	}

	if err := o.sc.Publish(o.subject, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (o *NatsStreamingOutput) Close() error {
	return o.sc.Close()
}

func (o *NatsStreamingOutput) lostHandler(conn stan.Conn, reason error) {
	o.l.Lock()
	defer o.l.Unlock()
	o.connP = false
	for {
		conn, err := stan.Connect(os.Getenv("STAN_CLUSTER_ID"), os.Getenv("STAN_CLIENT_ID"),
			stan.NatsURL(os.Getenv("NATS_URL")),
			stan.SetConnectionLostHandler(o.lostHandler),
		)
		if err != nil {
			log.Printf("Can't connect, retrying: %s", err)
			time.Sleep(5 * time.Second)
			continue
		}
		log.Printf("Reconnected")
		o.sc = conn
		o.connP = true
		return
	}
}

func (o *NatsStreamingOutput) connStatus() bool {
	o.l.RLock()
	defer o.l.RUnlock()
	return o.connP
}

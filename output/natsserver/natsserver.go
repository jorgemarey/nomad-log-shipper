package natsserver

import (
	"log"
	"sync"
	"time"

	"github.com/jorgemarey/nomad-log-shipper/output"
	nats "github.com/nats-io/nats.go"
)

type natsOutput struct {
	nc *nats.Conn

	connP bool
	l     sync.RWMutex
}

// NewNatsOutput return
func NewNatsOutput( /*TODO options*/ ) (output.Output, error) {
	output := &natsOutput{
		connP: true,
	}
	nc, err := nats.Connect(nats.DefaultURL, nats.PingInterval(10*time.Second), nats.MaxPingsOutstanding(5), nats.DisconnectHandler(func(nc *nats.Conn) {
		output.connChange(false)
	}), nats.ReconnectHandler(func(nc *nats.Conn) {
		output.connChange(true)
	}))
	if err != nil {
		log.Fatal(err)
	}
	output.nc = nc

	return output, err
}

func (o *natsOutput) Write(frame *output.LogFrame) error {
	for {
		if o.connStatus() {
			break
		}
	}

	err := o.nc.Publish("test", frame.Data)
	return err
}

func (o *natsOutput) Close() error {
	o.nc.Close()
	return nil
}

func (o *natsOutput) connChange(status bool) {
	o.l.Lock()
	defer o.l.Unlock()
	o.connP = status
}

func (o *natsOutput) connStatus() bool {
	o.l.RLock()
	defer o.l.RUnlock()
	return o.connP
}

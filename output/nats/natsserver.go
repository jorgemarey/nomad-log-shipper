package nats

import (
	"log"
	"sync"
	"time"

	"github.com/jorgemarey/nomad-log-shipper/output"
	nats "github.com/nats-io/nats.go"
)

type natsServerOutput struct {
	nc *nats.Conn

	connP bool
	l     sync.RWMutex
}

// NewNatsServerOutput return
func NewNatsServerOutput( /*TODO options*/ ) (output.Output, error) {
	output := &natsServerOutput{
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

func (o *natsServerOutput) Write(p []byte) (int, error) {
	for {
		if o.connStatus() {
			break
		}
	}

	err := o.nc.Publish("test", p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (o *natsServerOutput) Close() error {
	o.nc.Close()
	return nil
}

func (o *natsServerOutput) connChange(status bool) {
	o.l.Lock()
	defer o.l.Unlock()
	o.connP = status
}

func (o *natsServerOutput) connStatus() bool {
	o.l.RLock()
	defer o.l.RUnlock()
	return o.connP
}

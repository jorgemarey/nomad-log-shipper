package nats

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/jorgemarey/nomad-log-shipper/output"
	nats "github.com/nats-io/nats.go"
	stan "github.com/nats-io/stan.go"
)

type NatsStreamingOutput struct {
	nc             *nats.Conn
	sc             stan.Conn
	scMutex        *sync.RWMutex
	maxReconnect   int
	reconnectDelay time.Duration
	quitCh         chan struct{}
	ncStatus       bool
	scStatus       bool

	clusterID string
	clientID  string
	natsURL   string
}

type subjectOutput struct {
	*NatsStreamingOutput
	subject string
}

// NewNatsStreamingOutput return
func NewNatsStreamingOutput() (*NatsStreamingOutput, error) {
	o := &NatsStreamingOutput{
		scMutex:        &sync.RWMutex{},
		maxReconnect:   60,
		reconnectDelay: 2 * time.Second,
		quitCh:         make(chan struct{}),
		clusterID:      os.Getenv("STAN_CLUSTER_ID"),
		clientID:       os.Getenv("STAN_CLIENT_ID"),
		natsURL:        os.Getenv("NATS_URL"),
		ncStatus:       true, // initially set it to true
	}

	nc, err := nats.Connect(o.natsURL,
		nats.Name(o.clientID),
		nats.MaxReconnects(-1),
		nats.ReconnectBufSize(-1),
		nats.DisconnectHandler(func(nc *nats.Conn) {
			log.Printf("Disconnected from nats: %s", o.natsURL)
			o.scMutex.Lock()
			o.ncStatus = false
			o.scMutex.Unlock()
		}), nats.ReconnectHandler(func(nc *nats.Conn) {
			o.scMutex.Lock()
			o.ncStatus = true
			o.scMutex.Unlock()
			log.Printf("Reconnected to %s", o.natsURL)
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("can't connect to %s: %v", o.natsURL, err)
	}
	o.nc = nc

	return o, o.connect()
}

func (o *NatsStreamingOutput) Output(name string) output.Output {
	return &subjectOutput{
		NatsStreamingOutput: o,
		subject:             os.Getenv(fmt.Sprintf("STAN_SUBJECT_%s", name)),
	}
}

func (o *subjectOutput) Write(p []byte) (int, error) {
	n, err := o.write(p)
	if err != nil { // if the write fails, it could be a ping or commit error, try it once again
		return o.write(p)
	}
	return n, nil
}

func (o *subjectOutput) write(p []byte) (int, error) {
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
	o.scMutex.Lock()
	defer o.scMutex.Unlock()

	if o.sc == nil {
		return fmt.Errorf("conn is nil")
	}

	close(o.quitCh)
	err := o.sc.Close()
	if !o.nc.IsClosed() {
		o.nc.Close()
	}
	return err
}

func (o *NatsStreamingOutput) connStatus() bool {
	o.scMutex.RLock()
	defer o.scMutex.RUnlock()
	return o.ncStatus && o.scStatus
}

func (o *NatsStreamingOutput) connect() error {
	log.Printf("Connect: %s", o.natsURL)

	sc, err := stan.Connect(
		o.clusterID,
		o.clientID,
		stan.NatsConn(o.nc),
		//stan.NatsURL(o.natsURL),
		stan.SetConnectionLostHandler(func(conn stan.Conn, err error) {
			log.Printf("Disconnected from %s", o.natsURL)
			o.reconnect()
		}),
	)
	if err != nil {
		return fmt.Errorf("can't connect to %s: %v", o.natsURL, err)
	}

	o.scMutex.Lock()
	o.sc = sc
	o.scStatus = true
	o.scMutex.Unlock()
	return nil
}

func (o *NatsStreamingOutput) reconnect() {
	log.Printf("Reconnecting")
	o.scMutex.Lock()
	o.scStatus = false
	o.scMutex.Unlock()

	for i := 0; i < o.maxReconnect; i++ {
		select {
		case <-time.After(time.Duration(i) * o.reconnectDelay):
			if err := o.connect(); err == nil {
				log.Printf("Reconnecting (%d/%d) to %s succeeded", i+1, o.maxReconnect, o.natsURL)
				return
			}

			nextTryIn := time.Duration(i+1) * o.reconnectDelay
			log.Printf("Reconnecting (%d/%d) to %s failed", i+1, o.maxReconnect, o.natsURL)
			log.Printf("Waiting %s before next try", nextTryIn)
		case <-o.quitCh:
			log.Println("Received signal to stop reconnecting...")
			return
		}
	}
	log.Fatalf("Reconnecting limit (%d) reached", o.maxReconnect)
}

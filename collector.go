package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
	"time"

	nomad "github.com/hashicorp/nomad/api"
	"github.com/jorgemarey/nomad-log-shipper/output"
	"github.com/jorgemarey/nomad-log-shipper/storage"
)

// Collector is the one dealing with the allocation log recollection
type Collector struct {
	alloc  *nomad.Allocation
	getter LogGetter
	logCh  chan<- *output.LogFrame

	wg       *sync.WaitGroup
	shutdown chan struct{}
	cancel   chan struct{}
	canceled bool
	mtx      sync.Mutex
	tasks    map[string]struct{}

	storer storage.Storer
}

// NewAllocCollector initializes the allocation log collector. This deals with all task logs for
// this allocation
func NewAllocCollector(alloc *nomad.Allocation, getter LogGetter, logCh chan<- *output.LogFrame, storer storage.Storer) *Collector {
	return &Collector{
		alloc:    alloc,
		getter:   getter,
		logCh:    logCh,
		wg:       &sync.WaitGroup{},
		shutdown: make(chan struct{}),
		cancel:   make(chan struct{}),
		tasks:    make(map[string]struct{}),
		storer:   storer,
	}
}

// Start begins the log collection
func (c *Collector) Start() bool {
	return c.Update(c.alloc)
}

// Update is meant to be triggered when there's a change in an allocation.
func (c *Collector) Update(alloc *nomad.Allocation) bool {
	updated := false
	for task, info := range alloc.TaskStates {
		if _, ok := c.tasks[task]; !ok && !info.StartedAt.IsZero() {
			meta := getMeta(alloc, task)
			if len(meta) > 0 { // TODO: provide meta to allocation task collector
				c.tasks[task] = struct{}{}
				go c.collectTaskLogs(task, meta)
				updated = true
			}
		}
	}
	return updated
}

// Stop forcefully stops the log recollection
func (c *Collector) Stop() {
	select {
	// If we're already in shutdown just wait
	case <-c.shutdown:
		c.wg.Wait()
		return
	default:
	}
	c.closeCancel()
	fmt.Printf("Closed cancel: %s\n", c.alloc.ID)
	c.Shutdown()
}

// Shutdown gracefully stops the log recollection
func (c *Collector) Shutdown() {
	close(c.shutdown)
	c.wg.Wait()
}

func (c *Collector) collectTaskLogs(task string, meta map[string]string) { // TODO: refactor this to avoid code repetition
	c.wg.Add(1)
	defer c.wg.Done()

	properties := getProperties(c.alloc, task)

	errP := newStreamProccessor(c.logCh, "stderr", task, meta, properties)
	go errP.Start()

	outP := newStreamProccessor(c.logCh, "stdout", task, meta, properties)
	go outP.Start()

	// origin must be start because in other case it will start from the end on the files (so some logs could be lost)
	stderr, errError := c.advanceFrames(task, "stderr", errP)
	stdout, outError := c.advanceFrames(task, "stdout", errP)

	for {
		// hearbeat frames are already processed by nomad client
		select {
		case frame := <-stderr:
			if frame.FileEvent != "" {
				continue // TODO
			}
			errP.Write(frame.Data)
			c.storer.Set(task, "stderr", &storage.Info{Offset: frame.Offset, File: frame.File})
		case frame := <-stdout:
			if frame.FileEvent != "" {
				continue // TODO
			}
			outP.Write(frame.Data)
			c.storer.Set(task, "stdout", &storage.Info{Offset: frame.Offset, File: frame.File})
		case msg := <-errError:
			fmt.Println(msg) // TODO
		case msg := <-outError:
			fmt.Println(msg) // TODO
		// we have been a time whitout getting logs. Check if we're asked to shutdown and in that case cancel and return
		case <-time.After(1 * time.Second): // TODO: think if there's a better way of doing this
			select {
			case <-c.shutdown:
				c.closeCancel()
				errP.Close()
				outP.Close()
				return
			default:
			}
		}
	}
}

func (c *Collector) advanceFrames(task, stream string, proc *streamProccessor) (<-chan *nomad.StreamFrame, <-chan error) {
	info := c.storer.Get(task, stream)
	if info == nil {
		return c.getter.Logs(c.alloc, true, task, stream, nomad.OriginStart, 0, c.cancel, nil)
	}
	frameCh, errCh := c.getter.Logs(c.alloc, true, task, stream, nomad.OriginStart, 0, c.cancel, nil)
	for f := range frameCh {
		if f.File != info.File { // TODO: change to detect the number
			continue
		}
		if f.Offset < info.Offset {
			continue
		}
		l := int64(len(f.Data))
		index := l - (f.Offset - info.Offset)
		if index < 0 { // this is just to avoid nomad bug
			fmt.Println("Index is negative")
			index = 0
		}
		proc.Write(f.Data[index:])
		break
	}
	return frameCh, errCh
}

func (c *Collector) closeCancel() error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.canceled {
		return nil
	}
	close(c.cancel)
	c.canceled = true
	return nil
}

type streamProccessor struct {
	scanner *bufio.Scanner
	reader  io.Reader
	io.WriteCloser

	logCh      chan<- *output.LogFrame
	kind       string
	properties map[string]string

	finish chan struct{}
}

// newStreamProccessor creates and initializes a log stream proccessor
func newStreamProccessor(logCh chan<- *output.LogFrame, kind, taskName string, meta, properties map[string]string) *streamProccessor {
	pr, pw := io.Pipe()
	scanner := bufio.NewScanner(pr)
	scanner.Split(sizeSpliter(maxLineSize, bufio.ScanLines))
	return &streamProccessor{
		scanner:     scanner,
		reader:      pr,
		WriteCloser: pw,
		logCh:       logCh,
		kind:        kind,
		properties:  properties,
		finish:      make(chan struct{}),
	}
}

func (p *streamProccessor) Start() {
	for p.scanner.Scan() {
		data := p.scanner.Bytes()
		copied := make([]byte, len(data))
		copy(copied, data) // we need to copy the data (the scanner uses the same buffer for all the parsing)

		p.logCh <- &output.LogFrame{
			Data:       copied,
			Stream:     p.kind,
			Properties: p.properties,
			Meta:       make(map[string]string),
		}
	}

	if p.scanner.Err() != nil {
		// We need to keep reading even if the scaner fails.
		// All the reader data must be consumed
		io.Copy(ioutil.Discard, p.reader)
	}
	close(p.finish)
}

func (p *streamProccessor) Stop() {
	p.Close()
	<-p.finish
}

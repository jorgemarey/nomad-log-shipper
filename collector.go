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
}

// NewAllocCollector initializes the allocation log collector. This deals with all task logs for
// this allocation
func NewAllocCollector(alloc *nomad.Allocation, getter LogGetter, logCh chan<- *output.LogFrame) *Collector {
	return &Collector{
		alloc:    alloc,
		getter:   getter,
		logCh:    logCh,
		wg:       &sync.WaitGroup{},
		shutdown: make(chan struct{}),
		cancel:   make(chan struct{}),
		tasks:    make(map[string]struct{}),
	}
}

// Start begins the log collection
func (c *Collector) Start() {
	c.Update(c.alloc)
}

// Update is meant to be triggered when there's a change in an allocation.
func (c *Collector) Update(alloc *nomad.Allocation) {
	for task, info := range alloc.TaskStates {
		if _, ok := c.tasks[task]; !ok && !info.StartedAt.IsZero() {
			c.tasks[task] = struct{}{}
			go c.collectTaskLogs(task)
		}
	}
}

// Stop forcefully stops the log recollection
func (c *Collector) Stop() {
	c.closeCancel()
	c.Shutdown()
}

// Shutdown gracefully stops the log recollection
func (c *Collector) Shutdown() {
	close(c.shutdown)
	c.wg.Wait()
}

func (c *Collector) collectTaskLogs(task string) { // TODO: refactor this to avoid code repetition
	c.wg.Add(1)
	defer c.wg.Done()

	errP := newStreamProccessor(c.logCh, "stderr", task, c.alloc)
	go errP.Start()

	outP := newStreamProccessor(c.logCh, "stdout", task, c.alloc)
	go outP.Start()

	// origin must be start because in other case it will start from the end on the files (so some logs could be lost)
	// TODO: work with the offset
	stderr, errError := c.getter.Logs(c.alloc, true, task, "stderr", nomad.OriginStart, 0, c.cancel, nil)
	stdout, outError := c.getter.Logs(c.alloc, true, task, "stdout", nomad.OriginStart, 0, c.cancel, nil)
	for {
		// hearbeat frames are already processed by nomad client
		select {
		case frame := <-stderr:
			if frame.FileEvent != "" {
				// TODO
				continue
			}
			errP.Write(frame.Data)
			fmt.Printf("Event: %s. Offset: %d. File: %s\n", frame.FileEvent, frame.Offset, frame.File)
		case frame := <-stdout:
			if frame.FileEvent != "" {
				// TODO
				continue
			}
			outP.Write(frame.Data)
			fmt.Printf("Event: %s. Offset: %d. File: %s\n", frame.FileEvent, frame.Offset, frame.File)
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
func newStreamProccessor(logCh chan<- *output.LogFrame, kind, taskName string, alloc *nomad.Allocation) *streamProccessor {
	properties := map[string]string{
		"allocation": alloc.ID,
		"task":       taskName,
		"group":      alloc.TaskGroup,
		"job":        alloc.JobID,
		"namespace":  alloc.Namespace,
	} // TODO: include more info like:
	// "alloc_index": data["NOMAD_ALLOC_INDEX"],
	// "dc":          data["NOMAD_DC"],
	// "region":      data["NOMAD_REGION"],

	pr, pw := io.Pipe()
	scanner := bufio.NewScanner(pr)
	scanner.Split(sizeSpliter(maxLineSize, bufio.ScanLines))
	return &streamProccessor{
		scanner:     scanner,
		reader:      pr,
		WriteCloser: pw,
		logCh:       logCh,
		properties:  properties,
		finish:      make(chan struct{}),
	}
}

func (p *streamProccessor) Start() {
	for p.scanner.Scan() {
		p.logCh <- &output.LogFrame{
			Data:       p.scanner.Bytes(),
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

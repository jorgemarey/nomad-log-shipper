package output

import (
	"io"
	"time"
)

// Output defines a way out getting the logs out
type Output interface {
	io.WriteCloser
}

// LogFrame defines the content of a Log entry received from nomad
type LogFrame struct {
	Data       []byte
	Stream     string
	Time       time.Time
	Properties map[string]string
	Meta       map[string]string
}

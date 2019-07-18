package output

// Output defines a way out getting the logs out
type Output interface {
	Write(frame *LogFrame) error
	Close() error
}

// LogFrame defines the content of a Log entry received from nomad
type LogFrame struct {
	Data       []byte
	Stream     string
	Properties map[string]string
	Meta       map[string]string
}

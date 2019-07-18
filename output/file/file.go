package file

import (
	"fmt"
	"os"

	"github.com/jorgemarey/nomad-log-shipper/output"
)

type fileOutput struct {
	f *os.File
}

// NewFileOutput return
func NewFileOutput(file string) (output.Output, error) {
	var f *os.File
	var err error

	switch file {
	case "stdout":
		f = os.Stdout
	case "stderr":
		f = os.Stderr
	default:
		f, err = os.Open(file)
	}

	return &fileOutput{f: f}, err
}

func (o *fileOutput) Write(frame *output.LogFrame) error {
	_, err := o.f.WriteString(fmt.Sprintf("%s\n", string(frame.Data)))
	return err
}

func (o *fileOutput) Close() error {
	return o.f.Close()
}

package file

import (
	"os"

	"github.com/jorgemarey/nomad-log-shipper/output"
)

type fileOutput struct {
	*os.File
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

	return &fileOutput{File: f}, err
}

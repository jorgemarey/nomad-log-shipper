package output

import (
	"io"
)

// Output defines a way out getting the logs out
type Output interface {
	io.WriteCloser
}

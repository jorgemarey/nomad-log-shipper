package storage

type Store interface {
	Initialize( /*config*/ ) error
	AllocationStorer(allocID string) Storer
	Close() error
}

type Storer interface {
	Set(task, stream string, info *Info)
	Get(task, stream string) *Info
}

type Info struct {
	Offset int64
	File   string
}

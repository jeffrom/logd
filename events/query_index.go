package events

type readIndex interface {
	Query(off uint64, messages int) (*partitionArgList, error)
	Push(off, part uint64, size, messages int) error
}

type QueryIndex struct {
}

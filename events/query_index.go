package events

type readIndex interface {
	Query(off uint64, messages int) (*partitionArgList, error)
}

type QueryIndex struct {
}

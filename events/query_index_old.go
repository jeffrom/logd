package events

import "github.com/jeffrom/logd/protocol"

type queryIndexOld struct {
	topic *topic

	// stuff that needs to work concurrently
	partArgBuf   *partitionArgList
	batchScanner *protocol.BatchScanner
}

func newQueryIndexOld(topic *topic) *queryIndexOld {
	return &queryIndexOld{}
}

func (q *queryIndexOld) Query(off uint64, messages int) (*partitionArgList, error) {

	return nil, nil
}

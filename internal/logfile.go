package internal

import "os"

type LogFile struct {
	*os.File
	limit   int64
	closefn func()
}

func NewLogFile(f *os.File) *LogFile {
	return &LogFile{File: f}
}

func (lf *LogFile) WithClose(closefn func()) *LogFile {
	lf.closefn = closefn
	return lf
}

func (lf *LogFile) Close() error {
	if lf.closefn != nil {
		lf.closefn()
		return nil
	}
	return lf.File.Close()
}

func (lf *LogFile) SetLimit(limit int64) {
	lf.limit = limit
}

func (lf *LogFile) SizeLimit() (int64, int64, error) {
	if lf.limit > 0 {
		return 0, lf.limit, nil
	}
	stat, err := lf.Stat()
	if err != nil {
		return -1, -1, err
	}
	return stat.Size(), 0, nil
}

func (lf *LogFile) AsFile() *LogFile {
	return lf
}

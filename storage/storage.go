package storage

import (
	"io"
	"os"
)

// LogLine is a single event from a logs collection.
// E.g. {"time": "2019-06-29T00:00:00+00:00", "ip": "0.0.0.0", "method": "GET", "url": "/test/url"}
type LogLine map[string]interface{}

// collectionReader is reading and parsing a collection file line by line.
type collectionReader struct {
	reader io.Reader
}

func newCollectionReader(path string) (*collectionReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return &collectionReader{reader: f}, nil
}

func (r *collectionReader) readLogLine() (LogLine, error) {
	return nil, nil
}

func appendLogLine(path string, line LogLine) error {
	return nil
}

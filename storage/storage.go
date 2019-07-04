package storage

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
)

// LogLine is a single event from a logs collection.
// E.g. {"time": "2019-06-29T00:00:00+00:00", "ip": "0.0.0.0", "method": "GET", "url": "/test/url"}
type LogLine map[string]interface{}

// collectionReader is reading and parsing a collection file line by line.
type collectionReader struct {
	scanner *bufio.Scanner
}

func newCollectionReader(reader io.Reader) *collectionReader {
	return &collectionReader{
		scanner: bufio.NewScanner(reader),
	}
}

func (r *collectionReader) readLogLine() (LogLine, error) {
	if !r.scanner.Scan() {
		if err := r.scanner.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}

	var logline LogLine
	line := r.scanner.Text()
	err := json.Unmarshal([]byte(line), &logline)
	if err != nil {
		return nil, err
	}
	return logline, nil
}

func (r *collectionReader) readLogLines() ([]LogLine, error) {
	var loglines []LogLine
	for r.scanner.Scan() {
		var logline LogLine
		line := r.scanner.Text()
		err := json.Unmarshal([]byte(line), &logline)
		if err != nil {
			return nil, err
		}
		loglines = append(loglines, logline)
	}

	if err := r.scanner.Err(); err != nil {
		return nil, err
	}

	return loglines, nil
}

func appendLogLine(path string, line LogLine) error {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	jsonString, err := json.Marshal(line)
	if err != nil {
		return err
	}

	if _, err = file.Write(jsonString); err != nil {
		return err
	}
	if _, err = file.Write([]byte("\n")); err != nil {
		return err
	}
	return nil
}

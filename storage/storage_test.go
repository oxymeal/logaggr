package storage

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadLogLine(t *testing.T) {
	require := require.New(t)

	collectionFile := strings.Join([]string{
		"{\"a\": 1, \"b\": \"hello world\", \"c\": true}",
		"{\"d\": [1, 2, 3], \"e\": {\"ee\": 1}}",
	}, "\n")
	reader := newCollectionReader(strings.NewReader(collectionFile))

	line1, err := reader.readLogLine()
	require.Nil(err)
	require.Equal(LogLine{
		"a": 1.0,
		"b": "hello world",
		"c": true,
	}, line1)

	line2, err := reader.readLogLine()
	require.Nil(err)
	require.Equal(LogLine{
		"d": []interface{}{1.0, 2.0, 3.0},
		"e": map[string]interface{}{"ee": 1.0},
	}, line2)

	_, err = reader.readLogLine()
	require.Equal(err, io.EOF)
}

func TestReadLogLines(t *testing.T) {
	require := require.New(t)

	collectionFile := strings.Join([]string{
		"{\"a\": 1, \"b\": \"hello world\", \"c\": true}",
		"{\"d\": [1, 2, 3], \"e\": {\"ee\": 1}}",
	}, "\n")
	reader := newCollectionReader(strings.NewReader(collectionFile))

	lines, err := reader.readLogLines()
	require.Nil(err)
	require.Len(lines, 2)
	require.Equal(LogLine{
		"a": 1.0,
		"b": "hello world",
		"c": true,
	}, lines[0])
	require.Equal(LogLine{
		"d": []interface{}{1.0, 2.0, 3.0},
		"e": map[string]interface{}{"ee": 1.0},
	}, lines[1])
}

func TestAppendLogLine(t *testing.T) {
	require := require.New(t)

	// Create a temporary file and store its `path`
	file, err := ioutil.TempFile("", "TestAppendLogLine.txt")
	require.Nil(err)

	path := file.Name()
	err = file.Close()
	require.Nil(err)

	// Call appendLogLine
	err = appendLogLine(path, LogLine{
		"a": 1.0,
		"b": "hello world",
		"c": true,
	})
	require.Nil(err)

	// Read the file again
	content, err := ioutil.ReadFile(path)
	require.Nil(err)

	// Assert that its content equals appendLogLine argument
	var logLine LogLine
	err = json.Unmarshal(content, &logLine)
	require.Nil(err)
	require.Equal(LogLine{
		"a": 1.0,
		"b": "hello world",
		"c": true,
	}, logLine)
}

package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func makeColFilePath(name string) string {
	colDirPath, err := ioutil.TempDir("", "logaggr-test")
	if err != nil {
		panic(err)
	}

	return path.Join(colDirPath, fmt.Sprintf("%v.txt", name))
}

func TestCollectionServiceStop(t *testing.T) {
	require := require.New(t)
	colFilePath := makeColFilePath("TestCollectionServiceStop")

	service := NewCollectionService(colFilePath)
	require.True(service.IsRunning)

	service.Stop()
	require.False(service.IsRunning)
}

func TestCollectionServiceAppend(t *testing.T) {
	require := require.New(t)
	colFilePath := makeColFilePath("TestCollectionServiceAppend")

	service := NewCollectionService(colFilePath)
	err := service.Append(LogLine{
		"a": 1.0,
		"b": "Hello World",
	})
	require.Nil(err)

	file, err := os.Open(colFilePath)
	require.Nil(err)
	defer file.Close()

	reader := newCollectionReader(file)
	lines, err := reader.readLogLines()
	require.Nil(err)

	require.Len(lines, 1)
	require.Equal(LogLine{
		"a": 1.0,
		"b": "Hello World",
	}, lines[0])
}

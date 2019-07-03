package storage

import (
	"log"
)

// CollectionService runs in a background goroutines and provides functions
// to access a collections file (to read it, to search it and to write to it).
type CollectionService struct {
	RequestChan chan interface{}
}

// NewCollectionService creates and runs a CollectionService for a collection
// file with the given path.
func NewCollectionService(filePath string) *CollectionService {
	return nil
}

// Run runs background job of the CollectionService
func (s *CollectionService) Run() {
	for req := range s.RequestChan {
		if req, ok := req.(colStopRequest); ok {
			req.responseChan <- struct{}{}
			break
		} else {
			log.Printf("Unknown request type: %v", req)
		}
	}
}

type colStopRequest struct {
	responseChan chan<- colStopResponse
}

type colStopResponse struct{}

// Stop sends the request to stop the CollectionService and blocks until
// its complete.
func (s *CollectionService) Stop() {
	responseChan := make(chan colStopResponse)
	req := colStopRequest{responseChan}
	s.RequestChan <- req
	<-responseChan
}

// Append sends the request to append the line to the collection.
func (s *CollectionService) Append(logLine LogLine) error {
	return nil
}

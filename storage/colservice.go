package storage

import (
	"errors"
	"log"
	"time"
)

const ServiceTimeout = 5 * time.Second

var ErrTimeout = errors.New("Service Timeout")

// CollectionService runs in a background goroutines and provides functions
// to access a collections file (to read it, to search it and to write to it).
type CollectionService struct {
	RequestChan chan interface{}
	IsRunning   bool
	filePath    string
}

// NewCollectionService creates and runs a CollectionService for a collection
// file with the given path.
func NewCollectionService(filePath string) *CollectionService {
	service := &CollectionService{
		RequestChan: make(chan interface{}),
		IsRunning:   true,
		filePath:    filePath,
	}
	go service.Run()
	return service
}

// Run runs background job of the CollectionService
func (s *CollectionService) Run() {
	for req := range s.RequestChan {
		if req, ok := req.(colStopRequest); ok {
			s.IsRunning = false
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
func (s *CollectionService) Stop() error {
	responseChan := make(chan colStopResponse)
	req := colStopRequest{responseChan}
	s.RequestChan <- req

	select {
	case <-responseChan:
		return nil
	case <-time.After(ServiceTimeout):
		return ErrTimeout
	}
}

type colAppendRequest struct {
	responseChan chan<- colAppendResponse
	logLine      LogLine
}

type colAppendResponse struct {
	err error
}

// Append sends the request to append the line to the collection.
func (s *CollectionService) Append(logLine LogLine) error {
	responseChan := make(chan colAppendResponse)
	req := colAppendRequest{responseChan, logLine}
	s.RequestChan <- req

	select {
	case resp := <-responseChan:
		return resp.err
	case <-time.After(ServiceTimeout):
		return ErrTimeout
	}
}

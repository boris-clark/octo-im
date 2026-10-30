package types

import (
	"bytes"
	"reflect"
)

// StoreOperation identifies one owner-issued append attempt. Its request is
// copied at issuance and never shared with a worker or a decoded Event.
type StoreOperation struct{ request Event }

type storeResult struct {
	operation *StoreOperation
	event     Event
}

// CloneLogs copies entry identity and payload without sharing mutable bytes.
func CloneLogs(logs []Log) []Log {
	if logs == nil {
		return nil
	}
	result := append([]Log{}, logs...)
	for i := range result {
		result[i].Data = bytes.Clone(result[i].Data)
	}
	return result
}

func cloneStoreEvent(e Event) Event {
	e.Logs = CloneLogs(e.Logs)
	e.Config = e.Config.Clone()
	if e.TermStartIndexInfo != nil {
		e.TermStartIndexInfo = e.TermStartIndexInfo.Clone()
	}
	return e
}

// NewStoreOperation is called by the owner before dispatch. A newly minted
// operation cannot authenticate against a different owner's outstanding pointer.
func NewStoreOperation(request Event) (*StoreOperation, Event) {
	request.storeOperation, request.storeResult = nil, nil
	o := &StoreOperation{request: cloneStoreEvent(request)}
	request = cloneStoreEvent(request)
	request.storeOperation = o
	return o, request
}

// Request returns a detached copy for validation; callers cannot mutate authority.
func (o *StoreOperation) Request() Event {
	e := cloneStoreEvent(o.request)
	e.storeOperation = o
	return e
}

// StoreResponse seals the worker's terminal verdict to its original request.
// Mutating either the request before sealing or the response afterwards invalidates it.
func StoreResponse(request, response Event) Event {
	o := request.storeOperation
	request.storeOperation, request.storeResult = nil, nil
	if o == nil || !reflect.DeepEqual(o.request, request) {
		return response
	}
	response.storeOperation, response.storeResult = o, nil
	response.storeResult = &storeResult{operation: o, event: cloneStoreEvent(response)}
	return cloneStoreEvent(response)
}

// ValidResponse checks provenance and the sealed event before any owner state changes.
func (o *StoreOperation) ValidResponse(e Event) bool {
	if o == nil || e.storeOperation != o || e.storeResult == nil || e.storeResult.operation != o {
		return false
	}
	result := e.storeResult
	e.storeResult = nil
	return reflect.DeepEqual(result.event, e)
}

func SameLogs(a, b []Log) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Index != b[i].Index || a[i].Term != b[i].Term || a[i].Id != b[i].Id || !bytes.Equal(a[i].Data, b[i].Data) {
			return false
		}
	}
	return true
}

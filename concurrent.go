package bezsql

import (
	"database/sql"
	"time"
)

type ConcurrentFetchResult struct {
	Errors          []error
	RowChannel      chan *sql.Rows
	NextChannel     chan bool
	CompleteChannel chan bool
	CancelChannel   chan bool
}

type concurrentFetchChannelResponse struct {
	Index           int
	Errors          []error
	RowChannel      chan *sql.Rows
	NextChannel     chan bool
	CompleteChannel chan bool
	CancelChannel   chan bool
}

type concurrentFetchQueryChannelResponse struct {
	RowChannel      chan *sql.Rows
	NextChannel     chan bool
	CompleteChannel chan bool
	CancelChannel   chan bool
}

func executeReplica(db DB, responseChannel chan concurrentFetchQueryChannelResponse, errorChannel chan error) {
	successChannel, rowChannel, nextChannel, completeChannel, cancelChannel, queryErrorChannel := db.FetchConc()
	select {
	case err := <-queryErrorChannel:
		errorChannel <- err
	case <-successChannel:
		responseChannel <- concurrentFetchQueryChannelResponse{
			RowChannel:      rowChannel,
			NextChannel:     nextChannel,
			CompleteChannel: completeChannel,
			CancelChannel:   cancelChannel,
		}

	}
}

func runReplicas(dbs []DB) (*concurrentFetchQueryChannelResponse, []error) {
	successChannel := make(chan concurrentFetchQueryChannelResponse)
	errorChannel := make(chan error)

	for _, db := range dbs {
		go executeReplica(db, successChannel, errorChannel)
	}

	done := false
	errors := []error{}

	for i := 0; i < len(dbs); i++ {
		select {
		case err := <-errorChannel:
			errors = append(errors, err)
		case res := <-successChannel:
			if !done {
				done = true
				return &res, nil
			} else {
				res.CancelChannel <- true
			}
		}
	}
	return nil, errors
}

func replicateQuery(index int, query DB, resultChan chan concurrentFetchChannelResponse) {
	replicas := []DB{}

	for i := 0; i < 3; i++ {
		qc, _ := query.Clone()
		qc.RunParallel()
		replicas = append(replicas, qc)
	}

	rr, errors := runReplicas(replicas)

	if rr == nil {
		resultChan <- concurrentFetchChannelResponse{
			Index:  index,
			Errors: errors,
		}
	} else {
		resultChan <- concurrentFetchChannelResponse{
			Index:           index,
			RowChannel:      rr.RowChannel,
			NextChannel:     rr.NextChannel,
			CancelChannel:   rr.CancelChannel,
			CompleteChannel: rr.CompleteChannel,
		}
	}
}

func ConcurrentFetch(queries ...DB) (results map[int]ConcurrentFetchResult) {
	results = make(map[int]ConcurrentFetchResult)
	c := make(chan concurrentFetchChannelResponse)

	for i, query := range queries {
		go replicateQuery(i, query, c)
	}
	timeout := time.After(100 * time.Millisecond)
	for i := 0; i < len(queries); i++ {
		select {
		case fr := <-c:
			conRes := ConcurrentFetchResult{
				Errors:          fr.Errors,
				RowChannel:      fr.RowChannel,
				NextChannel:     fr.NextChannel,
				CompleteChannel: fr.CompleteChannel,
				CancelChannel:   fr.CancelChannel,
			}
			results[fr.Index] = conRes
		case <-timeout:
			return

		}

	}
	return
}

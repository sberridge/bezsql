package bezsql

import (
	"database/sql"
	"time"
)

type concurrentFetchResult struct {
	Errors           []error
	StartRowsChannel chan bool
	RowChannel       chan *sql.Rows
	NextChannel      chan bool
	CompleteChannel  chan bool
	CancelChannel    chan bool
}

type concurrentFetchChannelResponse struct {
	Index            int
	Errors           []error
	RowChannel       chan *sql.Rows
	StartRowsChannel chan bool
	NextChannel      chan bool
	CompleteChannel  chan bool
	CancelChannel    chan bool
}

func executeReplica(db DB, responseChannel chan concurrentFetchChannelResponse, errorChannel chan error) {
	successChannel, startRowsChannel, rowChannel, nextChannel, completeChannel, cancelChannel, queryErrorChannel := db.FetchConcurrent()
	select {
	case err := <-queryErrorChannel:
		errorChannel <- err
	case <-successChannel:
		responseChannel <- concurrentFetchChannelResponse{
			StartRowsChannel: startRowsChannel,
			RowChannel:       rowChannel,
			NextChannel:      nextChannel,
			CompleteChannel:  completeChannel,
			CancelChannel:    cancelChannel,
		}

	}
}

func runReplicas(dbs []DB) (*concurrentFetchChannelResponse, []error) {
	successChannel := make(chan concurrentFetchChannelResponse)
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

	//isn't liking replication atm
	for i := 0; i < 1; i++ {
		qc, _ := query.Clone()
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
			Index:            index,
			StartRowsChannel: rr.StartRowsChannel,
			RowChannel:       rr.RowChannel,
			NextChannel:      rr.NextChannel,
			CancelChannel:    rr.CancelChannel,
			CompleteChannel:  rr.CompleteChannel,
		}
	}
}

func ConcurrentFetch(queries ...DB) (results map[int]concurrentFetchResult) {
	results = make(map[int]concurrentFetchResult)
	c := make(chan concurrentFetchChannelResponse)

	for i, query := range queries {
		go replicateQuery(i, query, c)
	}
	timeout := time.After(100 * time.Millisecond)
	for i := 0; i < len(queries); i++ {
		select {
		case fr := <-c:
			conRes := concurrentFetchResult{
				Errors:           fr.Errors,
				StartRowsChannel: fr.StartRowsChannel,
				RowChannel:       fr.RowChannel,
				NextChannel:      fr.NextChannel,
				CompleteChannel:  fr.CompleteChannel,
				CancelChannel:    fr.CancelChannel,
			}
			results[fr.Index] = conRes
		case <-timeout:
			return

		}

	}
	return
}

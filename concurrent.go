package bezsql

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type ConcurrentFetchResult struct {
	Results   *sql.Rows
	CloseFunc context.CancelFunc
}

type concurrentFetchChannelResponse struct {
	Index     int
	Results   *sql.Rows
	CloseFunc context.CancelFunc
}

func runReplicas(dbs []DB) concurrentFetchChannelResponse {
	c := make(chan concurrentFetchChannelResponse)
	done := false
	doFetch := func(index int) {
		db := dbs[index]
		res, close, err := db.Fetch()
		if err != nil {
			fmt.Println(err)
			return
		}

		if done {
			close()
			return
		}

		qr := concurrentFetchChannelResponse{
			Index:     index,
			Results:   res,
			CloseFunc: close,
		}
		c <- qr
		done = true
	}
	for i := range dbs {
		go doFetch(i)
	}

	return <-c
}

func ConcurrentFetch(queries ...DB) (results map[int]ConcurrentFetchResult) {
	results = make(map[int]ConcurrentFetchResult)
	c := make(chan concurrentFetchChannelResponse)

	doFetch := func(index int) {

		q := queries[index]

		replicas := []DB{}

		for i := 0; i < 3; i++ {
			qc, _ := q.Clone()
			qc.RunParallel()
			replicas = append(replicas, qc)
		}

		rr := runReplicas(replicas)

		c <- concurrentFetchChannelResponse{
			Index:     index,
			Results:   rr.Results,
			CloseFunc: rr.CloseFunc,
		}
	}

	for i := range queries {
		go doFetch(i)
	}
	timeout := time.After(100 * time.Millisecond)
	for i := 0; i < len(queries); i++ {
		select {
		case fr := <-c:
			conRes := ConcurrentFetchResult{
				CloseFunc: fr.CloseFunc,
				Results:   fr.Results,
			}
			results[fr.Index] = conRes
		case <-timeout:
			return

		}

	}
	return
}

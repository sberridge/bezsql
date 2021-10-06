package bezsql

import (
	"context"
	"database/sql"
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

func ConcurrentFetch(queries ...DB) (results []ConcurrentFetchResult) {
	c := make(chan concurrentFetchChannelResponse)

	doFetch := func(index int) {
		res, closeF, _ := queries[index].Fetch()
		cr := concurrentFetchChannelResponse{
			Index:     index,
			Results:   res,
			CloseFunc: closeF,
		}
		c <- cr
	}

	for i := range queries {
		go doFetch(i)
	}

	for i := 0; i < len(queries); i++ {
		fr := <-c
		conRes := ConcurrentFetchResult{
			CloseFunc: fr.CloseFunc,
			Results:   fr.Results,
		}
		if len(results) < fr.Index {
			results = append(results, conRes)
		} else {
			results = append(results[:fr.Index+1], results[fr.Index:]...)
			results[fr.Index] = conRes
		}

	}
	return
}

package main

import (
	"log"
	"os"
	"time"
)

func main() {
	p, err := setupPG(os.Getenv("LR_DB_URL"))
	if err != nil {
		log.Fatalln(err)
	}

	jobs := newRunInfo([]xact{defaultXact(), pgbenchXact(1)})
	work := run{
		schedule: ctrlData{
			workers:   1,
			frequency: time.Second,
			pause:     false,
		},
		work: jobs,
	}

	control := make(chan ctrlData)

	go dispatch(p, work, control)

	runApi(jobs, control)

	p.Close()
}

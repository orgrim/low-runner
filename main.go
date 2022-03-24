package main

import (
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
	"log"
	"os"
	"sync"
	"time"
)

func setupPG(connstring string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(connstring)
	if err != nil {
		return nil, err
	}

	conn, err := pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func main() {
	p, err := setupPG(os.Getenv("LR_DB_URL"))
	if err != nil {
		log.Fatalln(err)
	}

	xacts := []xact{defaultXact(), pgbenchXact(1)}
	jobs := make(map[string]xact)
	control := make(chan work)

	for _, x := range xacts {
		jobs[x.Id] = x
	}

	go dispatch(p, jobs, 1, control)

	runApi(jobs, control)

	p.Close()
}

type work struct {
	workers   int
	frequency time.Duration
	pause     bool
}

// Keep a list of xact to run on the workers and schedule runs
func dispatch(pool *pgxpool.Pool, jobs map[string]xact, numWorker int, ctrl chan work) {
	if numWorker < 1 {
		log.Println("bad param for dispatch, workers:", numWorker)
		return
	}

	res := make(chan xactResult)
	wg := &sync.WaitGroup{}
	done := make(chan struct{})
	tick := time.NewTicker(5 * time.Second)
	pause := false

	go gather(res)

	for {
		if !pause {
			// log.Println("scheduling run")
			for _, v := range jobs {
				for i := 0; i < numWorker; i++ {
					go worker(pool, v, wg, res)
				}
			}

			go func(c chan struct{}) {
				wg.Wait()
				c <- struct{}{}
			}(done)

		}

		// use a flag to keep waiting if the workers have finished before the
		// ticker
		waitNextTick := true
	out:
		for {
			select {
			case <-done:
				// log.Println("run done")
				waitNextTick = false

			case <-tick.C:
				// log.Println("received tick:", waitNextTick)
				if !waitNextTick || pause {
					break out
				}

			case todo := <-ctrl:
				log.Println("received work update")
				if todo.workers > 0 {
					log.Printf("will spawn %d workers from now on", todo.workers)
					numWorker = todo.workers
				}

				if todo.frequency > 0 {
					log.Printf("will schedule run every %s from now on", todo.frequency)
					tick.Reset(todo.frequency)
				}

				if pause != todo.pause {
					log.Printf("pause is now: %v", todo.pause)
					pause = todo.pause
				}

				break out
			}
		}

	}
}

// Get a xact to run, run it and send the result
func worker(pool *pgxpool.Pool, job xact, wg *sync.WaitGroup, results chan xactResult) {
	wg.Add(1)
	r, err := runXact(job, pool)
	if err != nil {
		log.Printf("xact run failed: %s", err)
	}

	results <- r

	wg.Done()
}

// Gather the results from workers and compute stats
func gather(results chan xactResult) {
	count := 0
	tick := time.NewTicker(time.Second)
	xacts := make([]int, 0)

	failures := make([]xactResult, 0)

	for {

	out:
		for {
			select {
			case res := <-results:
				// log.Printf("xact=%s total=%v, pg=%v\n", res.xactId, res.endTime.Sub(res.startTime), res.endTime.Sub(res.beginTime))
				if res.outcome == Rollback {
					failures = append(failures, res)
				} else {
					count++
				}

				select {
				case <-tick.C:
					break out
				default:
				}
			case <-tick.C:
				break out
			}
		}

		xacts = append(xacts, count)
		sum := 0.0
		for _, v := range xacts {
			sum += float64(v)
		}

		log.Printf("instant xacts/s=%d, 1m avg xacts/s=%.2f, failures=%d\n", count, sum/float64(len(xacts)), len(failures))
		count = 0

		if len(xacts) >= 60 {
			xacts = xacts[1:]
		}
	}
}

package main

import (
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"log"
	"sync"
	"time"
)

type run struct {
	m        *sync.RWMutex
	Schedule ctrlData `json:"schedule"`
	Work     runInfo  `json:"work"`
}

type ctrlData struct {
	Workers   int
	Frequency time.Duration
	Pause     bool
}

type runInfo struct {
	Xacts map[string]xact
}

func newRunInfo(xactList []xact) runInfo {
	r := runInfo{
		Xacts: make(map[string]xact),
	}

	for _, x := range xactList {
		r.Xacts[x.id] = x
	}

	return r
}

func (r runInfo) get(xid string) (xact, error) {
	x, ok := r.Xacts[xid]
	if !ok {
		return xact{}, fmt.Errorf("xact not found in run list")
	}

	return x, nil
}

func (r runInfo) add(x xact) error {
	_, ko := r.Xacts[x.id]
	if ko {
		return fmt.Errorf("xact already exists in run list")
	}

	r.Xacts[x.id] = x

	return nil
}

func (r runInfo) remove(xid string) error {
	_, ok := r.Xacts[xid]
	if !ok {
		return fmt.Errorf("xact not found in run list")
	}

	delete(r.Xacts, xid)

	return nil
}

func (r runInfo) appendXact(xid string, x xact) (xact, error) {
	cur, ok := r.Xacts[xid]
	if !ok {
		return xact{}, fmt.Errorf("xact not found in run list")
	}

	for _, s := range x.Statements {
		cur.Statements = append(cur.Statements, s)
	}

	// When the list of statements is changed, the source and id of the
	// xact must be updated
	cur.genSource()

	// As the id changes, the old key must be removed and a new one created
	delete(r.Xacts, xid)
	r.Xacts[cur.id] = cur

	return cur, nil
}

// Keep a list of xact to run on the workers and schedule runs
func dispatch(pool *pgxpool.Pool, todo *run, ctrl chan struct{}) {
	numWorker := todo.Schedule.Workers
	if numWorker < 1 {
		log.Println("bad param for dispatch, workers:", numWorker)
		return
	}

	frequency := todo.Schedule.Frequency
	pause := false

	res := make(chan xactResult)
	wg := &sync.WaitGroup{}
	done := make(chan struct{})
	tick := time.NewTicker(frequency)

	go gather(res)

	for {
		// launch workers
		if !pause {
			todo.m.RLock()
			for _, v := range todo.Work.Xacts {
				for i := 0; i < numWorker; i++ {
					go worker(pool, v, wg, res)
				}
			}
			todo.m.RUnlock()

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
				// All workers are done
				waitNextTick = false

			case <-tick.C:
				// We got a tick, start the next run only if all workers are done
				if !waitNextTick || pause {
					break out
				}

			case <-ctrl:
				// process change in schedule
				todo.m.RLock()
				if numWorker != todo.Schedule.Workers {
					log.Printf("will spawn %d workers from now on", todo.Schedule.Workers)
					numWorker = todo.Schedule.Workers

					if pool.Config().MaxConns != int32(numWorker) {
						log.Println("reconnecting to adapt pool size")
						var err error
						pool, err = updatePoolConfig(pool, numWorker)
						if err != nil {
							log.Println(err)
						}
					}
				}

				if frequency != todo.Schedule.Frequency {
					log.Printf("will schedule run every %s from now on", todo.Schedule.Frequency)

					frequency = todo.Schedule.Frequency
					tick.Reset(frequency)
				}

				if pause != todo.Schedule.Pause {
					log.Printf("pause is now: %v", todo.Schedule.Pause)
					pause = todo.Schedule.Pause
				}
				todo.m.RUnlock()
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

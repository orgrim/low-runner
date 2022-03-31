package main

import (
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"log"
	"sync"
	"time"
)

type run struct {
	schedule ctrlData
	work     runInfo
}

type ctrlData struct {
	workers   int
	frequency time.Duration
	pause     bool
}

type runInfo struct {
	m     *sync.Mutex
	xacts map[string]xact
}

func newRunInfo(xactList []xact) runInfo {
	r := runInfo{
		m:     &sync.Mutex{},
		xacts: make(map[string]xact),
	}

	for _, x := range xactList {
		r.xacts[x.Id] = x
	}

	return r
}

func (r runInfo) get(xid string) (xact, error) {
	x, ok := r.xacts[xid]
	if !ok {
		return xact{}, fmt.Errorf("xact not found in run list")
	}

	return x, nil
}

func (r runInfo) add(x xact) error {
	_, ko := r.xacts[x.Id]
	if ko {
		return fmt.Errorf("xact already exists in run list")
	}

	r.m.Lock()
	r.xacts[x.Id] = x
	r.m.Unlock()

	return nil
}

func (r runInfo) remove(xid string) error {
	_, ok := r.xacts[xid]
	if !ok {
		return fmt.Errorf("xact not found in run list")
	}

	r.m.Lock()
	delete(r.xacts, xid)
	r.m.Unlock()

	return nil
}

func (r runInfo) appendXact(xid string, x xact) (xact, error) {
	cur, ok := r.xacts[xid]
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
	r.m.Lock()
	delete(r.xacts, xid)
	r.xacts[cur.Id] = cur
	r.m.Unlock()

	return cur, nil
}

// Keep a list of xact to run on the workers and schedule runs
func dispatch(pool *pgxpool.Pool, todo run, ctrl chan ctrlData) {
	numWorker := todo.schedule.workers
	if numWorker < 1 {
		log.Println("bad param for dispatch, workers:", numWorker)
		return
	}

	res := make(chan xactResult)
	wg := &sync.WaitGroup{}
	done := make(chan struct{})
	tick := time.NewTicker(todo.schedule.frequency)
	pause := false

	go gather(res)

	for {
		if !pause {
			// log.Println("scheduling run")
			for _, v := range todo.work.xacts {
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

			case newSched := <-ctrl:
				log.Println("received work update")
				if newSched.workers > 0 {
					log.Printf("will spawn %d workers from now on", newSched.workers)
					numWorker = newSched.workers

					if pool.Config().MaxConns != int32(numWorker) {
						log.Println("reconnecting to adapt pool size")
						var err error
						pool, err = updatePoolConfig(pool, numWorker)
						if err != nil {
							log.Println(err)
						}
					}
				}

				if newSched.frequency > 0 {
					log.Printf("will schedule run every %s from now on", newSched.frequency)
					tick.Reset(newSched.frequency)
				}

				if pause != newSched.pause {
					log.Printf("pause is now: %v", newSched.pause)
					pause = newSched.pause
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

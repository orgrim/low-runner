package main

import (
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"log"
	"sync"
	"time"
)

type run struct {
	Schedule ctrlData `json:"schedule"`
	Work     runInfo  `json:"work"`
}

type ctrlData struct {
	Workers   int
	Frequency time.Duration
	Pause     bool
}

func (c ctrlData) MarshalJSON() ([]byte, error) {
	m := struct {
		Workers   int    `json:"workers"`
		Frequency string `json:"frequency"`
		Pause     bool   `json:"pause"`
	}{
		Workers:   c.Workers,
		Frequency: c.Frequency.String(),
		Pause:     c.Pause,
	}

	return json.Marshal(m)
}

func (c *ctrlData) UnmarshalJSON(data []byte) error {
	var m map[string]interface{}

	err := json.Unmarshal(data, &m)
	if err != nil {
		return err
	}

	for k, v := range m {
		switch k {
		case "workers":
			w, ok := v.(float64)
			if ok {
				c.Workers = int(w)
			} else {
				fmt.Errorf("invalid value for workers")
			}
		case "frequency":
			s, ok := v.(string)
			if ok {
				f, err := time.ParseDuration(s)
				if err != nil {
					return err
				}

				c.Frequency = f
			} else {
				fmt.Errorf("invalid value for frequency")
			}
		case "pause":
			b, ok := v.(bool)
			if ok {
				c.Pause = b
			} else {
				fmt.Errorf("invalid value for pause")
			}
		default:
			return fmt.Errorf("invalid key in JSON")

		}
	}

	return nil
}

type runInfo struct {
	m     *sync.Mutex
	Xacts map[string]xact
}

func (r runInfo) MarshalJSON() ([]byte, error) {
	m := struct {
		Xacts []xact `json:"xacts"`
	}{
		Xacts: make([]xact, 0, len(r.Xacts)),
	}

	for _, v := range r.Xacts {
		m.Xacts = append(m.Xacts, v)
	}

	return json.Marshal(m)
}

func (r *runInfo) UnmarshalJSON(data []byte) error {
	var m struct {
		Xacts []xact `json:"xacts"`
	}

	err := json.Unmarshal(data, &m)
	if err != nil {
		return err
	}

	r.Xacts = make(map[string]xact)

	for _, v := range m.Xacts {
		v.genSource()
		r.Xacts[v.id] = v
	}

	return nil
}

func newRunInfo(xactList []xact) runInfo {
	r := runInfo{
		m:     &sync.Mutex{},
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

	r.m.Lock()
	r.Xacts[x.id] = x
	r.m.Unlock()

	return nil
}

func (r runInfo) remove(xid string) error {
	_, ok := r.Xacts[xid]
	if !ok {
		return fmt.Errorf("xact not found in run list")
	}

	r.m.Lock()
	delete(r.Xacts, xid)
	r.m.Unlock()

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
	r.m.Lock()
	delete(r.Xacts, xid)
	r.Xacts[cur.id] = cur
	r.m.Unlock()

	return cur, nil
}

// Keep a list of xact to run on the workers and schedule runs
func dispatch(pool *pgxpool.Pool, todo *run, ctrl chan ctrlData) {
	numWorker := todo.Schedule.Workers
	if numWorker < 1 {
		log.Println("bad param for dispatch, workers:", numWorker)
		return
	}

	res := make(chan xactResult)
	wg := &sync.WaitGroup{}
	done := make(chan struct{})
	tick := time.NewTicker(todo.Schedule.Frequency)
	pause := false

	go gather(res)

	for {
		if !pause {
			for _, v := range todo.Work.Xacts {
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
				// All workers are done
				waitNextTick = false

			case <-tick.C:
				// We got a tick, start the next run only if all workers are done
				if !waitNextTick || pause {
					break out
				}

			case newSched := <-ctrl:
				log.Println("received work update")
				if newSched.Workers > 0 {
					log.Printf("will spawn %d workers from now on", newSched.Workers)
					numWorker = newSched.Workers

					if pool.Config().MaxConns != int32(numWorker) {
						log.Println("reconnecting to adapt pool size")
						var err error
						pool, err = updatePoolConfig(pool, numWorker)
						if err != nil {
							log.Println(err)
						}
					}
				}

				if newSched.Frequency > 0 {
					log.Printf("will schedule run every %s from now on", newSched.Frequency)
					tick.Reset(newSched.Frequency)
				}

				if pause != newSched.Pause {
					log.Printf("pause is now: %v", newSched.Pause)
					pause = newSched.Pause
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

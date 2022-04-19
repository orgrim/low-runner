package main

import (
	"encoding/json"
	"fmt"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

type apiRun struct {
	Schedule apiSchedule `json:"schedule"`
	Work     apiWork     `json:"work"`
}

type apiSchedule struct {
	Workers   int    `json:"workers"`
	Frequency string `json:"frequency"`
	Pause     bool   `json:"pause"`
}

type apiWork struct {
	Xacts []apiXact `json:"xacts"`
}

type apiXact struct {
	Id         string   `json:"id,omitempty"`
	Outcome    string   `json:"outcome,omitempty"`
	Statements []string `json:"statements"`
}

type apiError struct {
	Error string `json:"error"`
}

func scheduleToApiSchedule(d ctrlData) apiSchedule {
	return apiSchedule{
		Workers:   d.Workers,
		Frequency: d.Frequency.String(),
		Pause:     d.Pause,
	}
}

func apiScheduleToSchedule(s apiSchedule) (ctrlData, error) {
	d := ctrlData{}

	f, err := time.ParseDuration(s.Frequency)
	if err != nil {
		return d, fmt.Errorf("invalid value for frequency")
	}

	if s.Workers < 1 {
		return d, fmt.Errorf("workers must be greater than or equal to 1")
	}

	d.Frequency = f
	d.Workers = s.Workers
	d.Pause = s.Pause

	return d, nil
}

func runInfoToApiWork(r runInfo, omitIds bool) apiWork {
	w := apiWork{
		Xacts: make([]apiXact, 0, len(r.Xacts)),
	}

	for _, v := range r.Xacts {
		ax := xactToApiXact(v)
		if omitIds {
			ax.Id = ""
		}

		w.Xacts = append(w.Xacts, ax)
	}

	return w
}

func apiWorkToRunInfo(a apiWork) runInfo {
	xl := make([]xact, 0, len(a.Xacts))

	for _, ax := range a.Xacts {
		xl = append(xl, apiXactToXact(ax))
	}

	return newRunInfo(xl)
}

func xactToApiXact(x xact) apiXact {
	ax := apiXact{Id: x.id, Outcome: string(x.Outcome)}
	stmts := make([]string, 0)
	for _, s := range x.Statements {
		stmts = append(stmts, s.Text)
	}

	ax.Statements = stmts
	return ax
}

func apiXactToXact(a apiXact) xact {
	x := newXact(a.Statements)

	if a.Outcome != "" {
		x.Outcome = xactOutcome(a.Outcome)
		x.genSource()
	}

	return x
}

// API actions: they all get the pointer to the run to edit it, the mutex must
// be used when reading and writing the run

func getXact(c echo.Context, r *run) error {
	id := c.Param("id")

	r.m.RLock()
	defer r.m.RUnlock()

	x, err := r.Work.get(id)
	if err != nil {
		return c.JSON(http.StatusNotFound, apiError{err.Error()})
	}

	ax := xactToApiXact(x)

	return c.JSON(http.StatusOK, ax)
}

func getAllXacts(c echo.Context, r *run) error {
	r.m.RLock()
	defer r.m.RUnlock()

	return c.JSON(http.StatusOK, runInfoToApiWork(r.Work, false))
}

func addXact(c echo.Context, r *run) error {
	ax := apiXact{}
	if err := c.Bind(&ax); err != nil {
		return c.JSON(http.StatusBadRequest, apiError{"missing or malformed payload"})
	}

	x := newXact(ax.Statements)

	r.m.Lock()
	err := r.Work.add(x)
	r.m.Unlock()

	if err != nil {
		return c.JSON(http.StatusConflict, apiError{err.Error()})
	}

	ax.Id = x.id

	return c.JSON(http.StatusCreated, ax)
}

func updateXact(c echo.Context, r *run) error {
	id := c.Param("id")

	ax := apiXact{}
	if err := c.Bind(&ax); err != nil {
		return c.JSON(http.StatusBadRequest, apiError{"missing or malformed payload"})
	}

	x := newXact(ax.Statements)

	r.m.Lock()
	newX, err := r.Work.appendXact(id, x)
	r.m.Unlock()

	if err != nil {
		return c.JSON(http.StatusNotFound, apiError{err.Error()})
	}

	return c.JSON(http.StatusOK, xactToApiXact(newX))
}

func replaceXact(c echo.Context, r *run) error {
	id := c.Param("id")

	ax := apiXact{}
	if err := c.Bind(&ax); err != nil {
		return c.JSON(http.StatusBadRequest, apiError{"missing or malformed payload"})
	}

	r.m.Lock()
	defer r.m.Unlock()

	if err := r.Work.remove(id); err != nil {
		return c.JSON(http.StatusNotFound, apiError{err.Error()})
	}

	x := newXact(ax.Statements)
	if err := r.Work.add(x); err != nil {
		return c.JSON(http.StatusBadRequest, apiError{err.Error()})
	}

	// Id has changed since statements have changed
	ax.Id = x.id
	return c.JSON(http.StatusOK, ax)
}

func removeXact(c echo.Context, r *run) error {
	id := c.Param("id")

	r.m.Lock()
	defer r.m.Unlock()

	if err := r.Work.remove(id); err != nil {
		return c.JSON(http.StatusNotFound, apiError{err.Error()})
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func getSchedule(c echo.Context, r *run) error {
	r.m.RLock()
	defer r.m.RUnlock()

	return c.JSON(http.StatusOK, scheduleToApiSchedule(r.Schedule))
}

func updateSchedule(c echo.Context, r *run, ctrl chan struct{}) error {
	w := apiSchedule{}
	if err := c.Bind(&w); err != nil {
		log.Println("could not bind input:", err)
		return c.JSON(http.StatusBadRequest, apiError{"missing or malformed payload"})
	}

	s, err := apiScheduleToSchedule(w)
	if err != nil {
		return c.JSON(http.StatusBadRequest, apiError{fmt.Sprintf("malformed payload: %s", err)})
	}

	r.m.Lock()
	r.Schedule = s
	r.m.Unlock()

	// signal the dispatch that the schedule changed, to make it avoid
	// check it every loop
	ctrl <- struct{}{}

	return c.JSON(http.StatusOK, struct{}{})
}

func dumpRun(c echo.Context, r *run) error {
	r.m.RLock()
	d := apiRun{
		Schedule: scheduleToApiSchedule(r.Schedule),
		Work:     runInfoToApiWork(r.Work, true),
	}

	r.m.RUnlock()

	return c.JSON(http.StatusOK, d)
}

func loadRun(c echo.Context, r *run, ctrl chan struct{}) error {
	nar := apiRun{}
	if err := c.Bind(&nar); err != nil {
		log.Println("could not bind input:", err)
		return c.JSON(http.StatusBadRequest, apiError{"missing or malformed payload"})
	}

	s, err := apiScheduleToSchedule(nar.Schedule)
	if err != nil {
		return c.JSON(http.StatusBadRequest, apiError{fmt.Sprintf("malformed payload: %s", err)})
	}

	nr := run{
		Schedule: s,
		Work:     apiWorkToRunInfo(nar.Work),
	}

	// we have to keep the mutex by copying its pointer before replacing
	// the contents of the run pointer
	mx := r.m
	mx.Lock()
	*r = nr
	r.m = mx
	mx.Unlock()

	ctrl <- struct{}{}

	return c.JSON(http.StatusOK, r)
}

// runApi starts the echo web server after linking all api functions to api
// endpoints
func runApi(hostPort string, todo *run, ctrl chan struct{}) {
	e := echo.New()

	e.HideBanner = true
	e.HidePort = true

	// Middleware
	e.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{
		Format: "${time_rfc3339} ${remote_ip} ${latency_human} ${method} ${uri} ${status} ${error}\n",
	}))
	e.Use(middleware.Recover())

	// Routes
	e.GET("/v1/xacts", func(c echo.Context) error { return getAllXacts(c, todo) })
	e.POST("/v1/xacts", func(c echo.Context) error { return addXact(c, todo) })
	e.GET("/v1/xacts/:id", func(c echo.Context) error { return getXact(c, todo) })
	e.PATCH("/v1/xacts/:id", func(c echo.Context) error { return updateXact(c, todo) }) // append queries
	e.PUT("/v1/xacts/:id", func(c echo.Context) error { return replaceXact(c, todo) })
	e.DELETE("/v1/xacts/:id", func(c echo.Context) error { return removeXact(c, todo) })

	e.GET("/v1/schedule", func(c echo.Context) error { return getSchedule(c, todo) })
	e.POST("/v1/schedule", func(c echo.Context) error { return updateSchedule(c, todo, ctrl) })

	e.GET("/v1/run", func(c echo.Context) error { return dumpRun(c, todo) })
	e.POST("/v1/run", func(c echo.Context) error { return loadRun(c, todo, ctrl) })

	// Start server
	log.Printf("HTTP REST API listening on %s", hostPort)
	e.Logger.Fatal(e.Start(hostPort))
}

func loadRunFromFile(path string) (run, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return run{}, fmt.Errorf("could not load file %s: %w", path, err)
	}

	ar := apiRun{}
	err = json.Unmarshal(data, &ar)
	if err != nil {
		return run{}, fmt.Errorf("could not parse JSON from %s: %w", path, err)
	}

	s, err := apiScheduleToSchedule(ar.Schedule)
	if err != nil {
		return run{}, fmt.Errorf("could not load schedule from file: %w", err)
	}

	r := run{
		m:        &sync.RWMutex{},
		Schedule: s,
		Work:     apiWorkToRunInfo(ar.Work),
	}

	return r, nil
}

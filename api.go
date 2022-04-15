package main

import (
	"fmt"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"log"
	"net/http"
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

func xactToApiXact(x xact) apiXact {
	ax := apiXact{Id: x.id, Outcome: string(x.Outcome)}
	stmts := make([]string, 0)
	for _, s := range x.Statements {
		stmts = append(stmts, s.Text)
	}

	ax.Statements = stmts
	return ax
}

// Wrapper to call a hanlder with a job list a parameter
func apiXactWrapHandler(uf func(echo.Context, runInfo) error, jobs runInfo) echo.HandlerFunc {
	return func(c echo.Context) error { return uf(c, jobs) }
}

func getXact(c echo.Context, jobs runInfo) error {
	id := c.Param("id")
	x, err := jobs.get(id)
	if err != nil {
		return c.JSON(http.StatusNotFound, apiError{err.Error()})
	}

	return c.JSON(http.StatusOK, xactToApiXact(x))
}

func getAllXacts(c echo.Context, jobs runInfo) error {
	axs := make(map[string]apiXact)
	for k, x := range jobs.Xacts {
		axs[k] = xactToApiXact(x)
	}

	return c.JSON(http.StatusOK, axs)
}

func addXact(c echo.Context, jobs runInfo) error {
	ax := apiXact{}
	if err := c.Bind(&ax); err != nil {
		return c.JSON(http.StatusBadRequest, apiError{"missing or malformed payload"})
	}

	x := newXact(ax.Statements)
	if err := jobs.add(x); err != nil {
		return c.JSON(http.StatusConflict, apiError{err.Error()})
	}

	ax.Id = x.id

	return c.JSON(http.StatusCreated, ax)
}

func updateXact(c echo.Context, jobs runInfo) error {
	id := c.Param("id")

	ax := apiXact{}
	if err := c.Bind(&ax); err != nil {
		return c.JSON(http.StatusBadRequest, apiError{"missing or malformed payload"})
	}

	x := newXact(ax.Statements)
	newX, err := jobs.appendXact(id, x)
	if err != nil {
		return c.JSON(http.StatusNotFound, apiError{err.Error()})
	}

	return c.JSON(http.StatusOK, xactToApiXact(newX))
}

func replaceXact(c echo.Context, jobs runInfo) error {
	id := c.Param("id")

	ax := apiXact{}
	if err := c.Bind(&ax); err != nil {
		return c.JSON(http.StatusBadRequest, apiError{"missing or malformed payload"})
	}

	if err := jobs.remove(id); err != nil {
		return c.JSON(http.StatusNotFound, apiError{err.Error()})
	}

	x := newXact(ax.Statements)
	if err := jobs.add(x); err != nil {
		return c.JSON(http.StatusBadRequest, apiError{err.Error()})
	}

	// Id has changed since statements have changed
	ax.Id = x.id
	return c.JSON(http.StatusOK, ax)
}

func removeXact(c echo.Context, jobs runInfo) error {
	id := c.Param("id")
	if err := jobs.remove(id); err != nil {
		return c.JSON(http.StatusNotFound, apiError{err.Error()})
	}

	return c.JSON(http.StatusOK, struct{}{})
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
	newr := run{}
	if err := c.Bind(&newr); err != nil {
		log.Println("could not bind input:", err)
		return c.JSON(http.StatusBadRequest, apiError{"missing or malformed payload"})
	}

	mx := r.m
	mx.Lock()
	*r = newr
	r.m = mx
	mx.Unlock()

	ctrl <- struct{}{}

	return c.JSON(http.StatusOK, r)
}

func runApi(hostPort string, todo *run, ctrl chan struct{}) {
	e := echo.New()

	// Middleware
	e.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{
		Format: "${time_rfc3339} ${remote_ip} ${latency_human} ${method} ${uri} ${status} ${error}\n",
	}))
	e.Use(middleware.Recover())

	jobs := todo.Work

	// Routes
	e.GET("/v1/xacts", apiXactWrapHandler(getAllXacts, jobs))
	e.POST("/v1/xacts", apiXactWrapHandler(addXact, jobs))
	e.GET("/v1/xacts/:id", apiXactWrapHandler(getXact, jobs))
	e.PATCH("/v1/xacts/:id", apiXactWrapHandler(updateXact, jobs)) // append queries
	e.PUT("/v1/xacts/:id", apiXactWrapHandler(replaceXact, jobs))
	e.DELETE("/v1/xacts/:id", apiXactWrapHandler(removeXact, jobs))

	e.POST("/v1/schedule", func(c echo.Context) error { return updateSchedule(c, todo, ctrl) })

	e.GET("/v1/run", func(c echo.Context) error { return dumpRun(c, todo) })
	e.POST("/v1/run", func(c echo.Context) error { return loadRun(c, todo, ctrl) })

	// Start server
	e.Logger.Fatal(e.Start(hostPort))
}

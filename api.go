package main

import (
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"log"
	"net/http"
	"sync"
)

type apiXact struct {
	Id         string   `json:"id"`
	Statements []string `json:"statements"`
}

type apiError struct {
	Error string `json:"error"`
}

func xactToApiXact(x xact) apiXact {
	ax := apiXact{Id: x.id}
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

// Wrapper to call a hanlder with a job list a parameter
func apiWorkWrapHandler(uf func(echo.Context, chan ctrlData) error, ctrl chan ctrlData) echo.HandlerFunc {
	return func(c echo.Context) error { return uf(c, ctrl) }
}

func updateWork(c echo.Context, ctrl chan ctrlData) error {
	w := ctrlData{}
	if err := c.Bind(&w); err != nil {
		log.Println("could not bind input:", err)
		return c.JSON(http.StatusBadRequest, apiError{"missing or malformed payload"})
	}

	ctrl <- w

	return c.JSON(http.StatusOK, struct{}{})

}

func dumpRun(c echo.Context, r *run) error {
	return c.JSON(http.StatusOK, r)
}

func loadRun(c echo.Context, r *run, mx *sync.Mutex) error {
	newr := run{}
	if err := c.Bind(&newr); err != nil {
		log.Println("could not bind input:", err)
		return c.JSON(http.StatusBadRequest, apiError{"missing or malformed payload"})
	}

	mx.Lock()
	*r = newr
	mx.Unlock()

	return c.JSON(http.StatusOK, r)
}

func runApi(hostPort string, todo *run, ctrl chan ctrlData) {
	e := echo.New()

	// Middleware
	e.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{
		Format: "${time_rfc3339} ${remote_ip} ${latency_human} ${method} ${uri} ${status} ${error}\n",
	}))
	e.Use(middleware.Recover())

	jobs := todo.Work
	mx := &sync.Mutex{}

	// Routes
	e.GET("/v1/xacts", apiXactWrapHandler(getAllXacts, jobs))
	e.POST("/v1/xacts", apiXactWrapHandler(addXact, jobs))
	e.GET("/v1/xacts/:id", apiXactWrapHandler(getXact, jobs))
	e.PATCH("/v1/xacts/:id", apiXactWrapHandler(updateXact, jobs)) // append queries
	e.PUT("/v1/xacts/:id", apiXactWrapHandler(replaceXact, jobs))
	e.DELETE("/v1/xacts/:id", apiXactWrapHandler(removeXact, jobs))

	e.POST("/v1/schedule", apiWorkWrapHandler(updateWork, ctrl))

	e.GET("/v1/run", func(c echo.Context) error { return dumpRun(c, todo) })
	e.POST("/v1/run", func(c echo.Context) error { return loadRun(c, todo, mx) })

	// Start server
	e.Logger.Fatal(e.Start(hostPort))
}

package main

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"log"
	"time"
)

type apiXact struct {
	Id         string   `json:"id"`
	Statements []string `json:"statements"`
}

type apiError struct {
	Error string `json:"error"`
}

type apiWork struct {
	Workers   int    `json:"workers"`
	Frequency string `json:"frequency"`
	Pause     bool   `json:"pause"`
}

func xactToApiXact(x xact) apiXact {
	ax := apiXact{Id: x.Id}
	stmts := make([]string, 0)
	for _, s := range x.Statements {
		stmts = append(stmts, s.Text)
	}
	ax.Statements = stmts

	return ax
}

// Wrapper to call a hanlder with a job list a parameter
func apiXactWrapHandler(uf func(echo.Context, map[string]xact) error, jobs map[string]xact) echo.HandlerFunc {
	return func(c echo.Context) error { return uf(c, jobs) }
}

func getXact(c echo.Context, jobs map[string]xact) error {
	id := c.Param("id")

	x, ok := jobs[id]
	if !ok {
		return c.JSON(http.StatusNotFound, apiError{"xact not found in current job list"})
	}

	return c.JSON(http.StatusOK, xactToApiXact(x))
}

func getAllXacts(c echo.Context, jobs map[string]xact) error {
	axs := make(map[string]apiXact)
	for k, x := range jobs {
		axs[k] = xactToApiXact(x)
	}

	return c.JSON(http.StatusOK, axs)
}

func addXact(c echo.Context, jobs map[string]xact) error {
	ax := apiXact{}
	if err := c.Bind(&ax); err != nil {
		return c.JSON(http.StatusBadRequest, apiError{"missing or malformed payload"})
	}

	x := newXact(ax.Statements)

	_, ko := jobs[x.Id]
	if ko {
		return c.JSON(http.StatusConflict, apiError{"xact already exists in current job list"})
	}

	jobs[x.Id] = x

	ax.Id = x.Id

	return c.JSON(http.StatusCreated, ax)
}

func updateXact(c echo.Context, jobs map[string]xact) error {
	id := c.Param("id")

	x, ok := jobs[id]
	if !ok {
		return c.JSON(http.StatusNotFound, apiError{"xact not found in current job list"})
	}

	ax := apiXact{}
	if err := c.Bind(&ax); err != nil {
		return c.JSON(http.StatusBadRequest, apiError{"missing or malformed payload"})
	}

	for _, s := range ax.Statements {
		x.Statements = append(x.Statements, stmt{Text: s})
	}

	jobs[x.Id] = x

	return c.JSON(http.StatusOK, xactToApiXact(x))
}

func replaceXact(c echo.Context, jobs map[string]xact) error {
	id := c.Param("id")

	x, ok := jobs[id]
	if !ok {
		return c.JSON(http.StatusNotFound, apiError{"xact not found in current job list"})
	}

	ax := apiXact{}
	if err := c.Bind(&ax); err != nil {
		return c.JSON(http.StatusBadRequest, apiError{"missing or malformed payload"})
	}

	delete(jobs, id)

	x = newXact(ax.Statements)

	jobs[x.Id] = x

	// Id has changed since statements have changed
	ax.Id = x.Id
	return c.JSON(http.StatusOK, ax)
}

func removeXact(c echo.Context, jobs map[string]xact) error {
	id := c.Param("id")

	_, ok := jobs[id]
	if !ok {
		return c.JSON(http.StatusNotFound, apiError{"xact not found in current job list"})
	}

	delete(jobs, id)

	return c.JSON(http.StatusOK, struct{}{})
}

// Wrapper to call a hanlder with a job list a parameter
func apiWorkWrapHandler(uf func(echo.Context, chan work) error, ctrl chan work) echo.HandlerFunc {
	return func(c echo.Context) error { return uf(c, ctrl) }
}

func updateWork(c echo.Context, ctrl chan work) error {
	aw := apiWork{}
	if err := c.Bind(&aw); err != nil {
		log.Println("could not bind input:", err)
		return c.JSON(http.StatusBadRequest, apiError{"missing or malformed payload"})
	}

	w := work{
		workers: aw.Workers,
		pause:   aw.Pause,
	}

	if aw.Frequency != "" {
		f, err := time.ParseDuration(aw.Frequency)
		if err != nil {
			return c.JSON(http.StatusBadRequest, apiError{"missing or malformed payload"})
		}
		w.frequency = f
	}

	ctrl <- w

	return c.JSON(http.StatusOK, struct{}{})

}

func runApi(jobs map[string]xact, ctrl chan work) {
	e := echo.New()

	// Middleware
	e.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{
		Format: "${time_rfc3339} ${remote_ip} ${latency_human} ${method} ${uri} ${status} ${error}\n",
	}))
	e.Use(middleware.Recover())

	// Routes
	e.GET("/v1/xacts", apiXactWrapHandler(getAllXacts, jobs))
	e.POST("/v1/xacts", apiXactWrapHandler(addXact, jobs))
	e.GET("/v1/xacts/:id", apiXactWrapHandler(getXact, jobs))
	e.PATCH("/v1/xacts/:id", apiXactWrapHandler(updateXact, jobs)) // append queries
	e.PUT("/v1/xacts/:id", apiXactWrapHandler(replaceXact, jobs))
	e.DELETE("/v1/xacts/:id", apiXactWrapHandler(removeXact, jobs))

	e.POST("/v1/schedule", apiWorkWrapHandler(updateWork, ctrl))

	// Start server
	e.Logger.Fatal(e.Start(":1323"))
}

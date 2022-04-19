# Low-runner

Low-runner is simulation tool for PostgreSQL that run xacts/queries in a loop

WARNING: this tool is in early stage of development.

## Install

```
go get https://github.com/orgrim/low-runner
```

## Run

```
LOWRUNNER_DB_URL="host=/tmp port=13609 dbname=bench" low-runner
```

See usage with `--help`, each CLI option has a fallback environment
variable. The usual `PG*` environment variables are used if present as a
fallback.

## REST API

See `api.go` like a true devops ☮️

Manage transactions:

* `GET /v1/xacts`: list current xacts in the loop
* `POST /v1/xacts`: add a new xact to the loop
* `GET /v1/xacts/:id`: get a xact by id from the loop
* `PATCH /v1/xacts/:id`: append queries to a xact in the loop
* `PUT /v1/xacts/:id`: replace a xact in the loop
* `DELETE /v1/xacts/:id`: remove a xact from the loop

Change the schedule:

* `GET /v1/schedule`: show the workers, interval or pause the loop
* `POST /v1/schedule`: change the schedule, workers, interval or pause the loop

Change a whole run (xacts and schedule):

* `GET /v1/run`: dump the run
* `POST /v1/run`: load a new run

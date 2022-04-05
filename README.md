# Low-runner

Low-runner is simulation tool for PostgreSQL that run xacts/queries in a loop

## Install

```
go get https://github.com/orgrim/low-runner
```

## Run

```
LOWRUNNER_DB_URL="host=/tmp port=13609 dbname=bench" low-runner
```

See usage with `--help`, each CLI option has a fallback environment variable.

## REST API

See `api.go` like a true devops ☮️

* `GET /v1/xacts`: list current xacts in the loop
* `POST /v1/xacts`: add a new xact to the loop
* `GET /v1/xacts/:id`: get a xact by id from the loop
* `PATCH /v1/xacts/:id`: append queries to a xact in the loop
* `PUT /v1/xacts/:id`: replace a xact in the loop
* `DELETE /v1/xacts/:id`: remove a xact from the loop
* `POST /v1/schedule`: change the schedule, workers, interval or pause the loop

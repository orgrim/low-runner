package main

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"log"
	"strings"
	"time"
)

type xactOutcome string

const (
	NotRun   xactOutcome = "notrun"
	Commit               = "commit"
	Rollback             = "rollback"
	Idle                 = "idle"
)

// xact represents a set of SQL statement that must be executed inside a
// transaction.
type xact struct {
	// Unique identifier that permits to link results with the job and
	// access it from the REST API
	id string

	// Plain SQL text to run
	source string

	// List of individual SQL statements
	Statements []stmt `json:"statements"`

	// Expected outcome of the transation
	Outcome xactOutcome `json:"outcome"`
}

func (x xact) MarshalJSON() ([]byte, error) {
	m := struct {
		Outcome xactOutcome `json:"outcome"`
		Sql     []string    `json:"statements"`
	}{
		Outcome: x.Outcome,
		Sql:     make([]string, 0, len(x.Statements)),
	}

	for _, s := range x.Statements {
		m.Sql = append(m.Sql, s.Text)
	}

	return json.Marshal(m)
}

func (x *xact) UnmarshalJSON(data []byte) error {
	var m struct {
		Outcome xactOutcome `json:"outcome"`
		Sql     []string    `json:"statements"`
	}

	err := json.Unmarshal(data, &m)
	if err != nil {
		return err
	}

	x.Outcome = m.Outcome
	x.Statements = make([]stmt, 0, len(m.Sql))

	for _, v := range m.Sql {
		x.Statements = append(x.Statements, stmt{Text: v})
	}

	x.genSource()

	return nil
}

type stmt struct {
	id   string
	Text string `json:"sql"`
}

func defaultXact() xact {
	x := xact{
		Outcome: Commit,
		Statements: []stmt{
			{Text: "SELECT 1"},
			{Text: "SELECT * FROM generate_series(0, 150) i"},
		},
	}

	x.genSource()

	return x
}

func pgbenchXact(scale int) xact {

	aid := fmt.Sprintf("round(random() * 100000 * %d)", scale)
	bid := fmt.Sprintf("round(random() * %d)", scale)
	tid := fmt.Sprintf("round(random() * 10 * %d)", scale)
	delta := "round(random() * 10000 - 5000)"

	x := xact{
		Outcome: Commit,
		Statements: []stmt{
			{Text: fmt.Sprintf("UPDATE pgbench_accounts SET abalance = abalance + (%s) WHERE aid = (%s)", delta, aid)},
			{Text: fmt.Sprintf("SELECT abalance FROM pgbench_accounts WHERE aid = (%s)", aid)},
			{Text: fmt.Sprintf("UPDATE pgbench_tellers SET tbalance = tbalance + (%s) WHERE tid = (%s)", delta, tid)},
			{Text: fmt.Sprintf("UPDATE pgbench_branches SET bbalance = bbalance + (%s) WHERE bid = (%s)", delta, bid)},
			{Text: fmt.Sprintf("INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)", tid, bid, aid, delta)},
		},
	}

	x.genSource()

	return x
}

func newXact(sql []string) xact {
	x := xact{
		Outcome: Commit,
	}

	stmts := make([]stmt, 0)
	for _, s := range sql {
		stmts = append(stmts, stmt{Text: s})
	}

	x.Statements = stmts

	x.genSource()

	return x
}

func (x *xact) genSource() {
	src := "BEGIN;"

	for _, s := range x.Statements {
		s.Text = strings.TrimRight(s.Text, "\n\r\t ")
		if !strings.HasSuffix(s.Text, ";") {
			s.Text += ";"
		}

		src = fmt.Sprintf("%s\n%s", src, s.Text)
	}

	src = fmt.Sprintf("%s\n%s;", src, strings.ToUpper(string(x.Outcome)))

	x.source = src
	x.id = fmt.Sprintf("%x", sha1.Sum([]byte(src)))
}

type xactResult struct {
	// Id of the xact that produced this result
	xactId string

	// time when the connection was acquired
	startTime time.Time

	// time when the BEGIN statement returned from PostgreSQL
	beginTime time.Time

	// time when the COMMIT / ROLLBACK statement returned from PostgreSQL or when the connection was left idle in transaction
	endTime time.Time

	// the real outcome of the xact
	outcome xactOutcome
}

type stmtResult struct {
	stmtId    string
	startTime time.Time
	stopTime  time.Time
	count     int
	failed    bool
}

func runXact(x xact, pool *pgxpool.Pool) (xactResult, error) {
	res := xactResult{
		xactId:    x.id,
		startTime: time.Now(),
		outcome:   Rollback,
	}

	// We want to get a connection within 5 seconds
	ctxTimeout, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := pool.Acquire(ctxTimeout)
	if err != nil {
		return res, err
	}

	defer conn.Release()

	// Start the transaction and record the time after we got an answer
	tx, err := conn.Begin(context.Background())
	if err != nil {
		return res, err
	}

	res.beginTime = time.Now()

	res.outcome = Commit
	for _, s := range x.Statements {
		if _, err := runStatement(s, tx); err != nil {
			log.Printf("xact=%s rollbacked: %s", x.id, err)
			res.outcome = Rollback
		}
	}

	switch res.outcome {
	case Commit:
		tx.Commit(context.Background())
	case Rollback:
		tx.Rollback(context.Background())
	}

	res.endTime = time.Now()

	return res, nil
}

func runStatement(s stmt, tx pgx.Tx) (stmtResult, error) {
	res := stmtResult{
		stmtId:    s.id,
		startTime: time.Now(),
	}

	rows, err := tx.Query(context.Background(), s.Text)
	if err != nil {
		res.failed = true
		res.stopTime = time.Now()
		return res, err
	}

	for rows.Next() {
		res.count++
	}

	res.stopTime = time.Now()

	if rows.Err() != nil {
		res.failed = true
		return res, rows.Err()
	}

	return res, nil
}

func setupPG(connstring string, lazyConnect bool) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(connstring)
	if err != nil {
		return nil, err
	}

	config.LazyConnect = lazyConnect

	conn, err := pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func updatePoolConfig(pool *pgxpool.Pool, maxConns int) (*pgxpool.Pool, error) {
	if maxConns < 1 {
		return nil, fmt.Errorf("new pool size is too small")
	}

	config := pool.Config()
	config.MaxConns = int32(maxConns)

	pool.Close()

	return pgxpool.ConnectConfig(context.Background(), config)
}

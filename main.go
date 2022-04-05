package main

import (
	"fmt"
	"github.com/spf13/pflag"
	"log"
	"os"
	"time"
)

var version string = "0.1.0"

type config struct {
	apiListenAddr string
	workFilePath  string
	connstring    string
}

func processCli(args []string) config {
	var (
		showHelp, showVersion bool
		opts                  config
	)

	pflag.Usage = func() {
		fmt.Fprintf(os.Stderr, "low-runner runs some xacts on a PostgreSQL database\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n  low-runner [options]\n\nOptions:\n")
		pflag.CommandLine.SortFlags = false
		pflag.PrintDefaults()
	}

	pflag.StringVarP(&opts.apiListenAddr, "api-listen-addr", "l", ":1323", "listen address and port of the REST API (LOWRUNNER_API_LISTEN_ADDR)")
	pflag.StringVarP(&opts.workFilePath, "work-file", "f", "", "path to a JSON file storing xacts to run at startup (LOWRUNNER_WORK_FILE)")
	pflag.StringVarP(&opts.connstring, "db-url", "d", "", "connection string to PostgreSQL (LOWRUNNER_DB_URL)\n")
	pflag.BoolVar(&showHelp, "help", false, "print usage")
	pflag.BoolVar(&showVersion, "version", false, "print version\n")

	pflag.CommandLine.MarkHidden("work-file")

	pflag.CommandLine.Parse(args)

	if showHelp {
		pflag.Usage()
		os.Exit(0)
	}

	if showVersion {
		fmt.Printf("low-runner version %s\n", version)
		os.Exit(0)
	}

	pflag.VisitAll(func(f *pflag.Flag) {
		switch f.Name {
		case "api-listen-addr":
			envValue := os.Getenv("LOWRUNNER_API_LISTEN_ADDR")
			if !f.Changed && envValue != "" {
				opts.apiListenAddr = envValue
			}
		case "work-file":
			envValue := os.Getenv("LOWRUNNER_WORK_FILE")
			if !f.Changed && envValue != "" {
				opts.workFilePath = envValue
			}
		case "db-url":
			envValue := os.Getenv("LOWRUNNER_DB_URL")
			if !f.Changed && envValue != "" {
				opts.connstring = envValue
			}
		}
	})

	return opts
}

func loadWorkFromFile(path string) (run, error) {
	return run{}, fmt.Errorf("not implemented")
}

func defaulWork() run {
	return run{
		schedule: ctrlData{
			workers:   1,
			frequency: time.Second,
			pause:     false,
		},
		work: newRunInfo([]xact{defaultXact()}),
	}
}

func main() {
	opts := processCli(os.Args[1:])

	p, err := setupPG(opts.connstring)
	if err != nil {
		log.Fatalln(err)
	}

	work, err := loadWorkFromFile(opts.workFilePath)
	if err != nil {
		work = defaulWork()
	}

	control := make(chan ctrlData)

	go dispatch(p, work, control)

	runApi(opts.apiListenAddr, work, control)

	p.Close()
}

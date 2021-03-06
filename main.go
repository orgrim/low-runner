package main

import (
	"fmt"
	"github.com/spf13/pflag"
	"log"
	"os"
	"sync"
	"time"
)

var version string = "0.2.2"

type config struct {
	apiListenAddr string
	workFilePath  string
	connstring    string
	lazyConnect   bool
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
	pflag.StringVarP(&opts.connstring, "db-url", "d", "", "connection string to PostgreSQL (LOWRUNNER_DB_URL)")
	pflag.BoolVar(&opts.lazyConnect, "lazy-connect", false, "do not connect immediately (LOWRUNNER_LAZY_CONNECT)\n")
	pflag.BoolVar(&showHelp, "help", false, "print usage")
	pflag.BoolVar(&showVersion, "version", false, "print version\n")

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
		case "lazy-connect":
			envValue := os.Getenv("LOWRUNNER_LAZY_CONNECT")
			if !f.Changed && envValue != "" {
				if envValue != "no" && envValue != "false" && envValue != "0" {
					opts.lazyConnect = true
				}
			}
		}
	})

	return opts
}

func defaulWork() run {
	return run{
		m: &sync.RWMutex{},
		Schedule: ctrlData{
			Workers:   1,
			Frequency: time.Second,
			Pause:     false,
		},
		Work: newRunInfo([]xact{defaultXact()}),
	}
}

func main() {
	opts := processCli(os.Args[1:])

	p, err := setupPG(opts.connstring, opts.lazyConnect)
	if err != nil {
		log.Fatalln(err)
	}

	var work run
	if opts.workFilePath != "" {
		work, err = loadRunFromFile(opts.workFilePath)
		if err != nil {
			log.Println(err)
			work = defaulWork()
		}
	} else {
		work = defaulWork()
	}

	control := make(chan struct{})

	go dispatch(p, &work, control)

	runApi(opts.apiListenAddr, &work, control)

	p.Close()
}

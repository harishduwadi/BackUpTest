package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/colinmarc/hdfs"
	"github.com/robfig/cron"
	"github.com/testusr/BackUpTest/db"
)

func main() {

	backUp := new(backUpconfig)
	var err error

	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "Command Line Argument Expected!\nThe Command Line Arguments represents which pool we'll be adding")
		return
	}

	poolID := os.Args[1]

	backUp.DB = pgdb.New()
	if backUp.DB == nil {
		return
	}
	defer backUp.DB.Close()

	backUp.Client, err = hdfs.New("us-lax-9a-ym-00:8020")
	if err != nil {
		return
	}
	defer backUp.Client.Close()

	err = backUp.setUpTape(poolID)
	if err != nil {
		return
	}
	defer backUp.TapeConfig.CloseTape()

	// Channel used to for communication betweem makeJob go routine and exexJobs go routine
	makeJobCompleted := make(chan error)

	cron := cron.New()

	backUp.mutex = &sync.Mutex{}

	// Catching Signal Interrupt
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		backUp.cleanUp(cron, poolID)
		os.Exit(1)
	}()

	// Schedular is the one that sets the jobType later
	jobType := "Hourly"

	// Tests <------
	sch1 := "00 */02 * * * *"

	arr := []string{"/ccr/2017", "/ccr/2018/07", "/ccr/2018/02", "/ccr/2018/"}
	i := -1

	cron.AddFunc(sch1, func() {
		i = (i + 1) % 4
		cronJob(backUp, arr[i], poolID, jobType, makeJobCompleted)
	})
	// Tests <-------

	cron.Start()

	fmt.Println(currentTime)

	for {
		// Do nothing
		time.Sleep(20 * time.Second)
		// Testing
		if time.Now().In(time.UTC).After(currentTime.Add(30 * time.Minute)) {
			return
		}
	}
}

/**
Description:
	This function represents a specific type of backup: hourly, monthly... , which is specified by the parameter
	jobType
Parameters:
	mutex: represents the mutex that is used to synchronize the go routine that will add Jobs to the DB among the
		the different cronJob go routines.
	root: represents the root path which is walked by hdfs filepath.walk method
	client: represents the connection to the yarn hdfs
	db: package that is used to connect to the postgres DB and also perform db operations
	poolID: represents the type of backup (with respect to the tapes) being done
	jobType: reprents the type of backup that is ran from the cronJob schedular
	makeJobCompleted: represents the channel that is used for communcation betweeen the makeJob and execJob go routines
	tapeconfig: package that is used to perform tape operations
*/
func cronJob(backUp *backUpconfig, root string, poolID string, jobType string, makeJobCompleted chan error) {

	// Only one cronJob will run at a time
	backUp.mutex.Lock()

	go backUp.makeJobs(poolID, jobType, makeJobCompleted, root)

	if err := backUp.execJobs(poolID, makeJobCompleted); err != nil {
		return
	}

	backUp.mutex.Unlock()
}

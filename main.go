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

var activeThread int

func main() {

	backUpA := new(backUpconfig)
	backUpB := new(backUpconfig)

	var err error

	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "Command Line Argument Expected!\nThe Command Line Arguments represents which pool we'll be adding")
		return
	}

	poolID := os.Args[1]

	err = setupBackupConfig(backUpA, poolID)
	if err != nil {
		closeAll(backUpA)
	}
	defer closeAll(backUpA)

	pairPoolID, err := backUpA.DB.GetPair(poolID)
	if err != nil {
		return
	}

	err = setupBackupConfig(backUpB, pairPoolID)
	if err != nil {
		closeAll(backUpB)
	}
	defer closeAll(backUpB)

	// Channel used to for communication betweem makeJob go routine and exexJobs go routine
	makeJobCompletedA := make(chan error)
	makeJobCompletedB := make(chan error)

	cron := cron.New()

	backUpA.syncTapeChange = &sync.Mutex{}
	backUpB.syncTapeChange = backUpA.syncTapeChange

	// Catching Signal Interrupt
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		cron.Stop()
		backUpA.cleanUp(poolID)
		fmt.Println("Closed first, now start closing the second backup")
		backUpB.cleanUp(pairPoolID)
		os.Exit(1)
	}()

	// Schedular is the one that sets the jobType later
	jobType := "Hourly"

	// Tests <------
	sch1 := "00 */01 * * * *"

	arr := []string{"/ccr/2017", "/ccr/2018/07", "/ccr/2018/02", "/ccr/2018/"}
	i := -1
	j := -1

	cron.AddFunc(sch1, func() {
		activeThread = activeThread + 1
		defer func() {
			activeThread = activeThread - 1
		}()

		i = (i + 1) % 4
		cronJob(backUpA, arr[i], poolID, jobType, makeJobCompletedA)

	})
	cron.AddFunc(sch1, func() {
		activeThread = activeThread + 1
		defer func() {
			activeThread = activeThread - 1
		}()

		j = (j + 1) % 4
		cronJob(backUpB, arr[j], pairPoolID, jobType, makeJobCompletedB)
	})
	// Tests <-------

	cron.Start()

	fmt.Println(currentTime)

	for {
		// Do nothing
		time.Sleep(20 * time.Second)
		fmt.Println("Number of active threads", activeThread)
		// Testing
		if time.Now().In(time.UTC).After(currentTime.Add(30 * time.Minute)) {
			return
		}

		if backUpA.errorInTape && backUpB.errorInTape {
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
func cronJob(backUp *backUpconfig, root string, poolID string, jobType string, makeJobCompleted chan error) error {

	// Only one cronJob will run at a time
	backUp.syncMakeExecJob.Lock()
	defer backUp.syncMakeExecJob.Unlock()

	if backUp.signalInterruptChan || backUp.errorInTape {
		return nil
	}

	go backUp.makeJobs(poolID, jobType, makeJobCompleted, root)

	if err := backUp.execJobs(poolID, makeJobCompleted); err != nil {
		backUp.errorInTape = true
		if backUp.signalInterruptChan {
			fmt.Println(poolID, ": Found the error return from execJob. Passing channel")
			backUp.execJobClosed <- 1
		}
		fmt.Println(poolID, "In main cronJob function", err)
		return err
	}

	return nil

}

func setupBackupConfig(config *backUpconfig, poolID string) error {
	var err error
	config.DB, err = pgdb.New()
	if config.DB == nil {
		return err
	}
	config.Client, err = hdfs.New("us-lax-9a-ym-00:8020")
	if err != nil {
		return err
	}
	err = config.setUpTape(poolID)
	if err != nil {
		return err
	}
	config.syncMakeExecJob = &sync.Mutex{}

	config.execJobClosed = make(chan int)

	config.errorInTape = false
	config.signalInterruptChan = false

	return nil
}

func closeAll(config *backUpconfig) {
	if config.DB != nil {
		config.DB.Close()
	}
	if config.Client != nil {
		config.Client.Close()
	}
	if config.TapeConfig != nil {
		config.TapeConfig.CloseTape()
	}
}

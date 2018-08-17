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

var activeThreads int

var schedules = map[string]string{
	"2Mins": "00 */02 * * * *",
	//"Hourly":  "00 00 * * * *",
	//"Daily":   "00 00 23 * * *",
	//"Monthly": "00 00 23 1 * *",
	//"Yearly":  "00 00 23 1 1 *",
}

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
		backUpB.cleanUp(pairPoolID)
		os.Exit(1)
	}()

	arr := []string{"/ccr/2017", "/ccr/2018/07", "/ccr/2018/02", "/ccr/2018"}
	i := -1
	j := -1
	// For Testing Purpose

	for key, val := range schedules {
		cron.AddFunc(val, func() {
			activeThreads = activeThreads + 1
			defer func() {
				activeThreads = activeThreads - 1
			}()

			i = (i + 1) % 4
			cronJob(backUpA, arr[i], poolID, key, makeJobCompletedA)

		})
		cron.AddFunc(val, func() {
			activeThreads = activeThreads + 1
			defer func() {
				activeThreads = activeThreads - 1
			}()

			j = (j + 1) % 4
			cronJob(backUpB, arr[j], pairPoolID, key, makeJobCompletedB)
		})
	}

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
	backUp: represents the struct that has all the resources for backing up
	root: represents the root path which is walked by hdfs filepath.walk method
	poolID: represents the type of backup (with respect to the tapes) being done
	jobType: reprents the type of backup that is ran from the cronJob schedular
	makeJobCompleted: represents the channel that is used for communcation betweeen the makeJob and execJob go routines
*/
func cronJob(backUp *backUpconfig, root string, poolID string, jobType string, makeJobCompleted chan error) error {

	// Run only one cron Job of one pool type at a time
	backUp.syncMakeExecJob.Lock()
	defer backUp.syncMakeExecJob.Unlock()

	if backUp.signalInterruptChan {
		return nil
	}

	go backUp.makeJobs(poolID, jobType, makeJobCompleted, root)

	if err := backUp.execJobs(poolID, makeJobCompleted); err != nil {
		fmt.Println(poolID, err)
		upderr := backUp.DB.UpdateErrorInTapeReason(poolID, err.Error())
		fmt.Println(upderr)
		// Update the tape stating that there was an error in the tape
		backUp.execJobClosed <- 1
		return err
	}

	return nil

}

/**
Description:
	This function is used to set the member variable of the bakup config struct
Parameter:
	config: The backup config struct whose member that needs set up
	poolID: PoolID whose tapes needs to be set up in config struct
Retur:
	Error if any
*/
func setupBackupConfig(config *backUpconfig, poolID string) error {
	var err error
	config.DB, err = pgdb.New()
	if err != nil {
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

	config.signalInterruptChan = false

	return nil
}

/**
Description:
	This function is used to close the resources that were open during execution
Parameter:
	config: The struct whose resources needs closing
*/
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

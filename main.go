package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/colinmarc/hdfs"
	"github.com/robfig/cron"
	"github.com/testusr/BackUpTest/db"
)

var activeThreads int

var schedules = map[string]string{
	"2Mins": "00 */05 * * * *", // For testing purpose
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
		fmt.Fprintln(os.Stderr, `Command Line Argument Expected!
		The Command Line Arguments represents the pool pair in which we'll be adding data`)
		return
	}

	poolID := os.Args[1]
	_, err = strconv.Atoi(poolID)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Invalid argument, please enter a valid poolID by looking in the DB")
		return
	}

	err = setupBackupConfig(backUpA, poolID)
	if err != nil {
		fmt.Println(err)
		backUpA.closeAll()
		return
	}
	defer backUpA.closeAll()

	// Getting the pair's poolID, eg STA000L7 pair is STAB000L7
	pairPoolID, err := backUpA.DB.GetPair(poolID)
	if err != nil {
		return
	}

	err = setupBackupConfig(backUpB, pairPoolID)
	if err != nil {
		backUpB.closeAll()
		return
	}
	defer backUpB.closeAll()

	cron := cron.New()

	// Need to sync these two threads when tape change occurs, both tries to access same
	// file.
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

	// For testing purpose-- will be replaced with "/" directory
	arr := []string{"/ccr", "/prod"}
	i := -1
	j := -1
	// For testing purpose

	for scheduleType, scheduleInCronFormat := range schedules {
		cron.AddFunc(scheduleInCronFormat, func() {
			activeThreads = activeThreads + 1
			defer func() {
				activeThreads = activeThreads - 1
			}()

			i = (i + 1) % len(arr)
			cronJob(backUpA, arr[i], poolID, scheduleType)

		})
		cron.AddFunc(scheduleInCronFormat, func() {
			activeThreads = activeThreads + 1
			defer func() {
				activeThreads = activeThreads - 1
			}()

			j = (j + 1) % len(arr)
			cronJob(backUpB, arr[j], pairPoolID, scheduleType)
		})
	}

	cron.Start()
	defer cron.Stop()

	fmt.Println(currentTime)

	for {
		// Do nothing
		time.Sleep(20 * time.Second)

		if backUpA.errorEncountered && backUpB.errorEncountered {
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
func cronJob(backUp *backUpconfig, root string, poolID string, jobType string) error {

	// Run only one cron Job of one pool type at a time
	// Discreprancy when both cron thread are running and both try to write to
	// same tape and/or update the DB
	backUp.syncCronJobs.Lock()
	defer backUp.syncCronJobs.Unlock()

	// Start another cron Job only if there wasn't any signal Interrupt sent by user
	if backUp.signalInterruptChan {
		return nil
	}

	// channel used to signal the end of makeJob go routine
	makeJobCompleted := make(chan error)

	// channel used to signal the error encountered in execJob to makeJob
	errorWhileExecuting := make(chan error)

	go backUp.makeJobs(poolID, jobType, makeJobCompleted, root, errorWhileExecuting)

	if err := backUp.execJobs(poolID, makeJobCompleted, errorWhileExecuting); err != nil {
		fmt.Println(poolID, err)
		backUp.DB.UpdateErrorInTapeReason(poolID, err.Error())
		backUp.errorEncountered = true
		// If there is an error, sleep until the user sends a signal interrupt
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
	config.syncCronJobs = &sync.Mutex{}

	config.execJobClosed = make(chan int)

	config.signalInterruptChan = false

	return nil
}

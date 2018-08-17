package main

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/colinmarc/hdfs"
	"github.com/testusr/BackUpTest/db"
	"github.com/testusr/BackUpTest/tape"
)

// This represents the block size the tape drive uses to read and write data into/from the tape
var recordSize = 4096

// This constant is the specifier of the end of the tar file; 1024 bytes of 0 after file is copied
var tarEndPad = 1024

var currentTime time.Time

func init() {
	currentTime = time.Now().In(time.UTC)
}

type backUpconfig struct {
	Client              *hdfs.Client
	TapeConfig          *tape.Config
	DB                  *pgdb.DBConn
	syncMakeExecJob     *sync.Mutex
	syncTapeChange      *sync.Mutex
	signalInterruptChan bool
	execJobClosed       chan int
}

/**
Description:
	This go routine creates Jobs by walking the hdfs and adds them to the DB according to the jobType parameter
Parameters:
	(See cronJob)
*/
func (config *backUpconfig) makeJobs(poolID string, jobType string, makeJobCompleted chan error, root string) {
	err := config.Client.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if config.signalInterruptChan {
			return errors.New("Signal Interrupt")
		}
		// Each Job is a directory in HDFS
		if !info.IsDir() {
			return nil
		}
		pathspecid, schedule, err := config.DB.GetPathSpec(path)
		if err != nil {
			return err
		}
		// Check if the backup schedule of the directory is different
		if schedule != jobType {
			return nil
		}
		err = config.DB.AddJob(path, poolID, pathspecid)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		makeJobCompleted <- err
	}
	makeJobCompleted <- nil
}

/**
Description:
	This function get all the jobs (one at a time) from the DB and calls other function to execute it
Parameter:
	(See cronJob)
Return:
	error: any error occured while execution, or nil
*/
func (config *backUpconfig) execJobs(poolID string, makeJobCompleted chan error) error {

	jobCreationCompleted := false
	var errorMakingJobs error
	waitForMakeJob := make(chan int)

	defer func() {
		<-waitForMakeJob
	}()

	// This go routine waits for the completion of makeJob go routine, check to end the
	// forever loop below
	go func() {
		errorMakingJobs = <-makeJobCompleted
		jobCreationCompleted = true
		waitForMakeJob <- 1
	}()

	if err := config.TapeConfig.JumpToEOM(); err != nil {
		return err
	}

	for {

		if errorMakingJobs != nil {
			return errorMakingJobs
		}

		startTime := time.Now().In(time.UTC)

		_, tapeID, err := config.DB.GetTapeInfo(config.TapeConfig.TapePath)
		if err != nil {
			return err
		}

		// Get one (currently any) initialized Job from the DB
		aJob, err := config.DB.GetAJob(poolID, startTime)
		if err != nil {
			return err
		}

		// If there is no error, and all the jobs has been executed return nil
		if errorMakingJobs == nil && jobCreationCompleted && aJob == nil {
			return nil
		}
		// Continue the loop until makeJob routine adds a job to DB or sends complete signal
		if aJob == nil {
			continue
		}

		// Set up the writing of the acquired Job
		numOfFiles, err := config.execSingleJob(aJob.ID, aJob.Name, tapeID, poolID)

		duration := time.Now().In(time.UTC).Sub(startTime)

		if err != nil {
			updateErr := config.DB.UpdateJob(aJob.ID, aJob.Name, startTime, duration, numOfFiles, pgdb.States.InComplete, aJob.PoolID)
			if updateErr != nil {
				return updateErr
			}
			return err
		}
		err = config.DB.UpdateJob(aJob.ID, aJob.Name, startTime, duration, numOfFiles, pgdb.States.Complete, aJob.PoolID)
		if err != nil {
			return err
		}
	}
}

/**
Description:
	This function gets all the contents of the directory (Job) and calls other functions to write to the tape one by one
Parameter:
	(See cronJob)
Return:
	int: number of files that was written to the tape
	error: any error occured while execution, or nil
*/
func (config *backUpconfig) execSingleJob(jobID int, path string, tapeID int, poolID string) (int, error) {

	filesAdded := 0
	allFiles, err := config.Client.ReadDir(path)
	if err != nil {
		return filesAdded, err
	}

	if err := config.DB.AddJobTapeMap(path, jobID, tapeID); err != nil {
		return filesAdded, err
	}

	fmt.Println(poolID, path)

	// Get the time when directory "path" was last backed up
	lastExecTime, err := config.DB.GetLastExec(path, poolID)
	if err != nil {
		return filesAdded, err
	}

	for _, fileInfo := range allFiles {

		if config.signalInterruptChan {
			err := errors.New("Signal Interrupt")
			return filesAdded, err
		}

		if !config.checkBackUpNeeded(fileInfo, lastExecTime) {
			continue
		}

		fullPath := path + "/" + fileInfo.Name()

		fileReader, err := config.Client.Open(fullPath)
		if err != nil {
			return filesAdded, err
		}

		// Call writeOneFile function that streams bytes from hdfs to tape
		err = config.writeOneFile(fullPath, fileInfo, fileReader)
		if err != nil {
			if !strings.Contains(err.Error(), "no space left on device") {
				return filesAdded, err
			}

			newTapeID, err := config.changeTape(poolID)
			if err != nil {
				return filesAdded, err
			}

			fileReader.Close()

			err = config.restartJob(fullPath, fileInfo, path, jobID, newTapeID)
			if err != nil {
				return filesAdded, err
			}

			tapeID = newTapeID
		}

		err = config.DB.AddFile(fullPath, jobID, tapeID, config.TapeConfig.GetFileMarkNum())
		if err != nil {
			return filesAdded, err
		}

		filesAdded++

		if err := config.TapeConfig.WriteEOF(); err != nil {
			return filesAdded, err
		}

	}

	return filesAdded, nil
}

/**
Description:
	This function restarts the writing to the tape for a specific file. This function is called when tape has just been changed
	and complete file needs to be backed up to the tape
Parameter:
	(see cronJob)
Return:
	error if any
*/
func (config *backUpconfig) restartJob(fullPath string, fileInfo os.FileInfo, path string, jobID int, newTapeID int) error {
	fileReader, err := config.Client.Open(fullPath)
	if err != nil {
		return err
	}

	err = config.writeOneFile(fullPath, fileInfo, fileReader)
	if err != nil {
		return err
	}

	if err := config.DB.AddJobTapeMap(path, jobID, newTapeID); err != nil {
		return err
	}
	return nil
}

/**
Description:
	This function changes the tape whose poolID is sent as parameter
Parameter:
	The poolID that needs tape replacement
Return:
	int: new Tape's Id
	error if any
*/
func (config *backUpconfig) changeTape(poolID string) (int, error) {

	config.syncTapeChange.Lock()
	defer config.syncTapeChange.Unlock()

	fromSlot, newTapeID, err := config.DB.GetTapeFromPool(poolID)
	if err != nil {
		return -1, err
	}

	driveNum, tapeID, err := config.DB.GetTapeInfo(config.TapeConfig.TapePath)
	if err != nil {
		return -1, err
	}

	if err := config.TapeConfig.CloseTape(); err != nil {
		return -1, err
	}

	unloadTo, err := tape.GetAEmptySlot()
	if err != nil {
		return -1, err
	}

	err = config.unloadAndUpdate(driveNum, unloadTo, tapeID)
	if err != nil {
		return -1, err
	}

	err = config.loadAndUpdate(driveNum, fromSlot, newTapeID)
	if err != nil {
		return 0, err
	}

	config.TapeConfig.RetensionOfTape()

	return newTapeID, nil
}

/**
Description:
	This function loads a new tape to the tape drive, and updates the changes to the DB
Parameter:
	driveNum: Where the tape needs to be loaded
	fromSlot: Where the tape is taken from
	tapeID: The ID of the tape placed in the drive in the DB
Return:
	error if any
*/
func (config *backUpconfig) loadAndUpdate(driveNum int, fromSlot int, newTapeID int) error {
	err := tape.Load(driveNum, fromSlot)
	if err != nil {
		return err
	}

	// Setting the tape again, in the same file w.r.t file system, and same recordsize
	err = config.TapeConfig.DeepCopy(config.TapeConfig.TapePath)
	if err != nil {
		return err
	}

	err = config.DB.UpdateTapeTable(0, false, false, newTapeID)
	if err != nil {
		return err
	}
	err = config.DB.UpdateStorage(newTapeID, config.TapeConfig.TapePath)
	if err != nil {
		return err
	}

	return nil
}

/**
Description:
	This function unloads a tape to the tape drive, and updates the changes to the DB
Parameter:
	driveNum: Where the tape needs to be unloaded
	fromSlot: Where the tape is placed
	tapeID: The ID of the tape being taken out in the DB
Return:
	error if any
*/
func (config *backUpconfig) unloadAndUpdate(driveNum int, unloadTo int, tapeID int) error {
	err := tape.Unload(driveNum, unloadTo)
	if err != nil {
		return err
	}

	err = config.DB.UpdateTapeTable(unloadTo, true, false, tapeID)
	if err != nil {
		return err
	}

	// -1 representing the NULL value in the DB, when a tape is taken out from the tapeDrive
	err = config.DB.UpdateStorage(-1, config.TapeConfig.TapePath)
	if err != nil {
		return err
	}

	return nil
}

/**
Description:
	This function checks whether a file needs backing up
Parameter:
	fileInfo: represents struct that has information about the file
	lastExecTime: represents the time when the file's last backup was performed
Return:
	bool: true if file needs to be backup, false otherwise
*/
func (config *backUpconfig) checkBackUpNeeded(fileInfo os.FileInfo, lastExecTime time.Time) bool {
	if fileInfo.IsDir() {
		return false
	}
	// Only for testing purpose
	if fileInfo.Size() > 12000000 {
		return false
	}
	if (fileInfo.ModTime().In(time.UTC)).Before(lastExecTime) {
		return false
	}
	return true
}

/**
Description:
	This function does the actual reading from the hdfs and writing to the tape
Parameters:
	Path: represents the absolute path of the file that is being written to the tape
	fileheader: represents the struct that has information about the file
	fileReader: represents the io.Reader that will stream content of the file from hdfs
	tapeWriter: represtns the io.Writer that will stream the content of the tape
Return:
	error: any error occured while execution, or nil
*/
func (config *backUpconfig) writeOneFile(path string, fileheader os.FileInfo, fileReader *hdfs.FileReader) error {

	tw := tar.NewWriter(config.TapeConfig.TapeWriter)

	header := new(tar.Header)
	header.Name = path
	header.Size = fileheader.Size()
	header.Mode = int64(fileheader.Mode())
	header.ModTime = fileheader.ModTime()

	if err := tw.WriteHeader(header); err != nil {
		return err
	}

	if fileheader.Size() != 0 {
		if _, err := io.Copy(config.TapeConfig.TapeWriter, fileReader); err != nil {
			return err
		}

		// Pad to get the valid 512 block size of written data
		config.TapeConfig.TapeWriter.Write(make([]byte, (512 - (config.TapeConfig.TapeWriter.Buffered() % 512))))
	}

	config.TapeConfig.TapeWriter.Write(make([]byte, tarEndPad))

	_, err := config.TapeConfig.TapeWriter.Write(make([]byte, config.TapeConfig.TapeWriter.Available()))
	if err != nil {
		return err
	}

	err = config.TapeConfig.FlushBuffers()
	if err != nil {
		return err
	}

	return nil

}

/**
Description:
	This function gets the address of the tape in the /dev/ directory and opens tape for use
Parameters:
	PoolID: Which pool tape needs to be set
Return:
	tape.Config: represents the struct that will be used perform tape operations
	error: any error occured while execution, or nil
*/
func (config *backUpconfig) setUpTape(poolID string) error {
	tapePath, err := config.DB.GetStoragePath(poolID)
	if err != nil {
		return err
	}
	config.TapeConfig, err = tape.New(tapePath, recordSize)
	return nil
}

/**
Description:
	This function is called when signal interrupt occurs
		- Closes all the open resources
		- Changes the State of the in-progrss job to interrupted
Parameters:
	PoolID: Which backup pool needs to be closed
*/
func (config *backUpconfig) cleanUp(poolID string) {

	config.signalInterruptChan = true
	if activeThreads > 0 {
		<-config.execJobClosed
	}

	if err := config.DB.InterruptCloseJob(poolID); err != nil {
		log.Println(err)
	}

	config.TapeConfig.CloseTape()
	config.DB.Close()
	config.Client.Close()

}

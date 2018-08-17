package pgdb

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/lib/pq"
)

type DBConn struct {
	DBSql *sql.DB
}

type Job struct {
	ID                int
	Name              string
	StartTime         pq.NullTime
	DurationInMinutes sql.NullInt64
	NumOfFiles        sql.NullInt64
	State             string
	PoolID            int
	PathSpecID        int
}

var States State

type State struct {
	Initialized string
	InProgress  string
	Complete    string
	Interrupted string
	InComplete  string
}

/**
Description:
	Set up different possible states, so that there's no spelling mistake when user is typing the states
*/
func init() {
	States.Complete = "Complete"
	States.Initialized = "Initialized"
	States.InProgress = "In-Progress"
	States.Interrupted = "Interrupted"
	States.InComplete = "InComplete"
}

/**
Description:
	This method is used to update the tape table after tape has been changed.
Parameter:
	The attributes of the tape table
*/
func (db *DBConn) UpdateTapeTable(slotNum int, isFull bool, errorinTape bool, ID int) error {
	query := "UPDATE TAPE SET slotnumber=$1, isfull=$2, errorintape=$3 where id=$4"
	_, err := db.DBSql.Exec(query, slotNum, isFull, errorinTape, ID)
	if err != nil {
		return err
	}
	return nil
}

/**
Description:
	This method is used to update the storage table after the tape has been changed
Parameter:
	The attributes of the storage table
*/
func (db *DBConn) UpdateStorage(tapeID int, name string) error {
	if tapeID < 0 {
		query := "UPDATE Storage SET tapeid=NULL where name=$1"
		_, err := db.DBSql.Exec(query, name)
		if err != nil {
			return err
		}
		return nil
	}
	query := "UPDATE Storage SET tapeid=$1 where name=$2"
	_, err := db.DBSql.Exec(query, tapeID, name)
	if err != nil {
		return err
	}
	return nil
}

/**
Description:
	This method is used to the retrieve tapeID and the drive num of the
	tape at path "tapePath"
Parameter:
	The path of the drive whose infor we need
Return:
	The driveNum it correspondes to
	The id of the tape
	error if any
*/
func (db *DBConn) GetTapeInfo(tapePath string) (int, int, error) {
	query := "Select drivenumber, tapeid From storage where name=$1"
	row := db.DBSql.QueryRow(query, tapePath)

	var driveNum, tapeID int

	err := row.Scan(&driveNum, &tapeID)
	if err != nil {
		return -1, -1, err
	}
	return driveNum, tapeID, nil
}

/**
Description:
	This method is used to get another tape from certain pool
Parameter:
	PoolID: The pool from where we need additional tape
Return:
	The slot where the additional tape resides
	The tapeID of the tape
	error if any
*/
func (db *DBConn) GetTapeFromPool(poolID string) (int, int, error) {
	query := `SELECT slotnumber, id FROM Tape 
	WHERE poolid=$1 AND slotnumber <> 0 AND isFull=false AND errorintape=false ORDER BY name`

	row := db.DBSql.QueryRow(query, poolID)

	var fromslot, ID int

	err := row.Scan(&fromslot, &ID)
	if err != nil {
		return -1, -1, err
	}

	return fromslot, ID, nil
}

/**
Description:
	This method takes in path- some Job, and checks when it is supposed to run obtained from
	PathSpec table. If the PathSpec doesn't exist for some specific path, then it creates
	a new PathSpec entry for the Job with hourly backup schedule.
Parameters:
	Path: represents the absolute path of the Job/directory
Return:
	int: represents the unique pathspec id of the Job
	string: the specified backup schedule for the path/directory
	error: any error occured while execution, or ni
*/
func (db *DBConn) GetPathSpec(path string) (int, string, error) {
	query := "SELECT id, schedule From PathSpec WHERE name=$1"
	row := db.DBSql.QueryRow(query, path)

	var schedule string
	var id int

	err := row.Scan(&id, &schedule)
	if err == sql.ErrNoRows {
		// Need to make sure pathspec table has unique flag on for name
		err = db.AddPathSpec(path, "2Mins") // Yearly would be better??
		if err != nil {
			return -1, "", err
		}
		return db.GetPathSpec(path)
	}

	if err != nil {
		return -1, "", err
	}

	return id, schedule, nil
}

/**
Description:
	This method adds a row to the PathSpec Table
Parameter:
	The parameters represents the columns of the table
Return:
	error: any error occured while execution, or nil
*/
func (db *DBConn) AddPathSpec(path string, schedule string) error {
	query := "INSERT INTO PathSpec VALUES (DEFAULT, $1, $2)"
	_, err := db.DBSql.Exec(query, path, schedule)
	if err != nil {
		err2, _ := err.(*pq.Error)
		if err2.Code.Name() == "unique_violation" {
			return nil
		}
		return err
	}
	return nil
}

/**
Description:
	This method retrieves the startTime of the latest entry of a completed Job
Parameter:
	path: represents the absolute path of a directory/Job
	poolID: represents the type of backup with respect to the type of tape.
Return:
	time: The latest time when the path Job was performed to completion.
	error: any error occured while execution, or nil
*/
func (db *DBConn) GetLastExec(path string, poolID string) (time.Time, error) {
	query := "SELECT starttime FROM Job WHERE name=$1 AND poolID = $2 AND state=$3 ORDER BY startTime DESC"
	rows, err := db.DBSql.Query(query, path, poolID, States.Complete)
	if err != nil {
		return time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC), err
	}
	defer rows.Close()
	for rows.Next() {
		var t time.Time
		rows.Scan(&t)
		return t, nil
	}

	return time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC), nil
}

/**
Description:
	Thie method adds a new entry to the JobTapeMap Table
Parameter:
	The parameters represents the columns of the table.
Return:
	error: any error occured while execution, or nil
*/
func (db *DBConn) AddJobTapeMap(jobName string, jobID int, tapeID int) error {
	query := "INSERT INTO JobTapeMap VALUES(DEFAULT, $1, $2, $3)"
	_, err := db.DBSql.Exec(query, jobName, jobID, tapeID)
	if err != nil {
		return err
	}
	return nil
}

/**
Description:
	This method is called when signal interrupt occurs, and we need to close the job
	that is being performed.
Parameter:
	poolID: represents the type of backup and only the jobs that needs to be closed
Return:
	error: any error occured while execution, or nil
*/
func (db *DBConn) InterruptCloseJob(poolID string) error {
	query := "UPDATE Job SET state=$2 WHERE poolID=$1 AND state=$3"
	_, err := db.DBSql.Exec(query, poolID, States.Interrupted, States.InProgress)
	if err != nil {
		return err
	}
	return err
}

/**
Description:
	This method is used to update a job in the Job table
Parameters:
	The parameters are the column of the table.
Return:
	error: any error occured while execution, or nil
*/
func (db *DBConn) UpdateJob(id int, name string, startTime time.Time, duration time.Duration, numOfFiles int, state string, poolID int) error {
	query := "UPDATE Job SET startTime=$3, durationInMinutes=$4, numOfFiles=$5, state=$6 WHERE id=$1 AND NAME=$2 AND poolID=$7"
	_, err := db.DBSql.Exec(query, id, name, startTime, int(duration.Minutes()), numOfFiles, state, poolID)
	if err != nil {
		return err
	}
	return err
}

/**
Description:
	This method is used to get the path of tape drive according to the poolID sent as parameter
Parameter:
	poolID: represents the tape pool where is backup will be done
Return:
	string: The path of the tape drive
	int: The unique ID of the tape
	error: any error occured while execution, or nil
*/
func (db *DBConn) GetStoragePath(poolID string) (string, error) {
	query := "SELECT Storage.Name FROM Storage Join Pool ON Storage.Id = Pool.StorageId Where Pool.Id =$1"
	rows, err := db.DBSql.Query(query, poolID)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var path string

	if rows.Next() {
		err := rows.Scan(&path)
		if err != nil {
			return "", err
		}
	} else {
		return "", errors.New(`Tape From That Pool Is Not Loaded Or the DB is Not Updated! 
		Please load the Tape and update the DB`)
	}

	return path, rows.Err()
}

/**
Description:
	This method is used to get one initialized job from the DB. It will also update the state of
	the job that it just retrieved.
Parameter:
	poolID: represents the poolID whose job we need to perform
	startTime: represents that time that symbolizes the Job has not been scheduled
Return:
	*Job: The struct pointer that has the information about the Job that was just scheduled
	error: any error occured while execution, or nil
*/
func (db *DBConn) GetAJob(poolID string, startTime time.Time) (*Job, error) {
	query := "SELECT * FROM Job WHERE state='Initialized' AND poolID =$1 ORDER BY ID"
	rows, err := db.DBSql.Query(query, poolID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var tempJob Job
		err := rows.Scan(&tempJob.ID, &tempJob.Name, &tempJob.StartTime, &tempJob.DurationInMinutes, &tempJob.NumOfFiles, &tempJob.State, &tempJob.PoolID, &tempJob.PathSpecID)
		if err != nil {
			return nil, err
		}
		err = db.UpdateJob(tempJob.ID, tempJob.Name, startTime, 0, 0, States.InProgress, tempJob.PoolID)
		if err != nil {
			return nil, err
		}
		return &tempJob, nil
	}
	return nil, nil
}

/**
Description:
	This method adds a new entry to Job Table, but before adding it checks if it already exists.
Parameter:
	The parameters are the columns of table
Return:
	error: any error occured while execution, or nil
*/
func (db *DBConn) AddJob(name string, poolID string, pathspecid int) error {
	// Check if a job already exists
	query := "SELECT name FROM Job WHERE name=$1 AND poolid=$2 AND state=$3"
	row := db.DBSql.QueryRow(query, name, poolID, States.Initialized)
	var tempString string
	err := row.Scan(&tempString)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if err == nil {
		return nil
	}
	// Make a new job only if error is norow found
	query = "INSERT INTO JOB(id, name, state, poolid, pathspecid) VALUES (DEFAULT, $1, 'Initialized', $2, $3);"
	_, err = db.DBSql.Exec(query, name, poolID, pathspecid)
	if err != nil {
		return err
	}
	return nil
}

/**
Description:
	This method adds a new entry to File Table.
Parameter:
	The parameters are the columns of table
Return:
	error: any error occured while execution, or nil
*/
func (db *DBConn) AddFile(fileName string, jobID int, tapeID int, fileMarkNum int) error {
	query := "INSERT INTO File VALUES(DEFAULT, $1, $2, $3, $4)"
	_, err := db.DBSql.Exec(query, fileName, jobID, fileMarkNum, tapeID)
	if err != nil {
		return err
	}
	return nil
}

/**
Description:
	This function is used to get the tape from the pool that is for different location,
	so that we can simultaneously write on both tapes
Parameter:
	poolID: The pool where we'll user chose to write
Return:
	The tapeId, poolID, and slotnumber of tape for different location
*/
func (db *DBConn) GetPair(poolID string) (string, error) {
	query := "SELECT name FROM Tape WHERE poolid=$1 ORDER BY name"
	row := db.DBSql.QueryRow(query, poolID)
	var poolName string
	err := row.Scan(&poolName)
	if err != nil {
		return "", err
	}
	// Getting the string eg ST_000L7 (3rd slot represents the location)
	pairPoolName := poolName[:2] + "_" + poolName[3:]
	query = "SELECT poolid From Tape WHERE poolID<>$1 AND name LIKE $2 ORDER BY name"
	row = db.DBSql.QueryRow(query, poolID, pairPoolName)
	var pairPoolID int
	err = row.Scan(&pairPoolID)
	if err != nil {
		return "", err
	}
	return strconv.Itoa(pairPoolID), nil
}

/**
Description: This method is used to connect to the pg server
*/
func New() (*DBConn, error) {
	connStr := "user=admin password=password dbname=backupTest"

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return nil, err
	}

	return &DBConn{
		DBSql: db,
	}, nil
}

/**
Description:
	This method is used to close the connection to the pg server
*/
func (db *DBConn) Close() {
	db.DBSql.Close()
}

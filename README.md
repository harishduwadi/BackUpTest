# BackUpTest

## Setting Up the Environment: 

### OS:
The resources this program requires only works on Ubuntu 16.04.

### Go:
* Download Go archive from https://golang.org/dl/
* Extract the files by issuing:
``` $ tar -C /usr/local -xzf ~/Downloads/go$VERSION.$OS-$ARCH.tar.gz ```
* Add go binary file path to the Path environmnet variable; either in /etc/profile or .profile
``` $ echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.profile ``` <br />
The environmnet variable will be updated once we restart the system, or immediately (until the current <br />
terminal is open) by running the command
``` $ source ~/.profile ```

### Database:
* Creating a Postgres DB Account:
  * Download Postgres
  ``` $ sudo apt-get install postgresql postgresql-contrib ```
  * Creating a User/Role
  ``` $ sudo -u postgres createuser --interactive ```
  * Add user
  ``` $ sudo adduser (newuser) ```
* Creating DB with the new user
  * Switch to new user account
   ``` $ sudo -i -u (newuser-- admin)```
  * Issue createdb command
  ```$ createdb (dbname-- backupTest)```
* Creating Tables in the new DB
  * In the new user account
  ```$ psql (dbname) ```
  * Issue sql commands to create tables (below)

* Create Tables/Constraints And Adding Some Entries:
 ```
Create Table PathSpec (
	ID Serial Primary Key,
	Name varchar,
	Schedule varchar
);

Create Table Job (
	ID Serial Primary Key,
	Name varchar,
	NumOfFiles timestamp,
	State varchar,
	PoolID integer,
	PathSpecID integer
);

Create Table File (
	ID Serial Primary Key,
	Name varchar,
	JobID integer,
	FileMarkNum integer,
	TapeID integer
);

Create Table Tape (
	ID Serial Primary Key,
	Name varchar,
	PoolID integer,
	SlotNumber integer,
	IsFull boolean,
	ErrorInTape boolean
);

Create Table Pool (
	ID Serial Primary Key,
	Name varchar,
	StorageID integer
);

Create Table Storage (
	ID Serial Primary Key,
	Name varchar,
	TapeID integer,
	DriveNum integer
);

Create Table JobTapeMap (
	ID Serial Primary Key,
	Name varchar,
	JobID integer,
	TapeID integer
);

INSERT INTO Storage VALUES(DEFAULT, '/dev/nst0', 1, 0);
INSERT INTO Storage VALUES(DEFAULT, '/dev/nst1', 2, 1);
INSERT INTO Pool VALUES(DEFAULT, 'StagingA', 1);
INSERT INTO Pool VALUES(DEFAULT, 'StagingB', 2);
INSERT INTO Tape VALUES(DEFAULT, 'STA000L7', 1, 0, false, false);
INSERT INTO Tape VALUES(DEFAULT, 'STA001L7', 1, 3, false, false);
INSERT INTO Tape VALUES(DEFAULT, 'STB000L7', 2, 0, false, false);
INSERT INTO Tape VALUES(DEFAULT, 'STB001L7', 2, 4, false, false);

Alter Table Job Add Foreign Key (PoolID) references Pool(ID);
Alter Table Job Add Foreign Key (PathSpecID) references PathSpec(ID);
Alter Table File Add Foreign Key (JobID) references Job(ID);
Alter Table File Add Foreign Key (TapeID) references Tape(ID);
Alter Table Tape Add Foreign Key (PoolID) references Pool(ID);
Alter Table Pool Add Foreign Key (StorageID) references Storage(ID);
Alter Table Storage Add Foreign Key (TapeID) references Tape(ID);
Alter Table JobTapeMap Add Foreign Key (JobID) references Job(ID);
Alter Table JobTapeMap Add Foreign Key (TapeID) references Tape(ID);
```

### Virtual Tape (Will be replaced with the actual tape later)
* Getting the source code
``` $ git clone https://github.com/markh794/mhvtl ```

Note: The release version 1.5-3_release doesn’t have a bug fix, so need to use the master version
* Configuration Steps
  *Packages Needed
    * libz-dev
    * liblzo2-dev
    * sg3-utils
    * lsscsi
    * linux-libc-dev
    * mtx
    * mt-st
    * make
  * Steps: (Assuming git clone was done in ~ directory)
    * ``` $ cd ~/mhvtl/kernel ```
    * ``` $ make ```
    * ``` $ sudo adduser --system --group vtl ```
    * ``` $ sudo make install ```
    * ``` $ cd ~/mhvtl ```
    * ``` $ make ```
    * ``` $ sudo adduser vtl ```
    * ensure /etc/passwd vtl user has /bin/bash (sudo vim /etc/passwd ; search for vtl and change /bin/... to /bin/bash)
    * ensure /opt/mhvtl is owned by vtl user/group (sudo chown vtl: /opt/mhvtl)
    * ``` $ sudo make install ```
    * ``` $ sudo /etc/init.d/mhvtl start ``` 
    
At this point using lsscsi -g command we should be able to find 10 tapes. <br />


We’ll be using /dev/sg10 SCSI generic tape drive for loading and unloading tapes, and /dev/nst_ tape drive for encoding data into the tape. 

### Pre-Run SetUp
* Load Tape:
  * ``` $ mtx -f /dev/sg10 load 1 0 ```
  * ```$ mtx -f /dev/sg10 load 2 1 ```
  * ```$ mt -f /dev/nst0 rewind && mt -f /dev/nst0 erase ```
  * ```$ mt -f /dev/nst1 rewind && mt -f /dev/nst1 erase ```

### Run 
  * ``` go run BackupTest.go main.go 1 ``` <br />
(Here the arguments represents the tape pool, which we just loaded in pre-run step)


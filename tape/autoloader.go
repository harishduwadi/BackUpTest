package tape

import (
	"bytes"
	"os/exec"
	"strconv"
	"sync"
)

// Unload Method is used to unload a tape from drive "driveNum", and place the tape in slotnumber "slotNum"
func Unload(driveNum int, slotNum int) error {
	driveN := strconv.Itoa(driveNum)
	slotN := strconv.Itoa(slotNum)
	cmd := exec.Command("mtx", "-f", "/dev/sg10", "unload", slotN, driveN)
	err := cmd.Run()
	if err != nil {
		return err
	}
	return nil
}

// GetMTXStatus is used to get the status of the tape library, which slot are empty, full, and name of tape if present
func GetMTXStatus() (string, error) {

	cmd := exec.Command("mtx", "-f", "/dev/sg10", "status")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return "", err
	}

	return out.String(), nil
}

// TODO: need to find how does the tape drive get paired with particular file

// Load is used to load a tape form slot number "SlotNum" to drive "driveNum"
func Load(driveNum int, slotNum int) error {
	driveN := strconv.Itoa(driveNum)
	slotN := strconv.Itoa(slotNum)
	cmd := exec.Command("mtx", "-f", "/dev/sg10", "load", slotN, driveN)
	err := cmd.Run()
	for ind, val := range reservedSlot {
		if val == slotNum {
			reservedSlot = append(reservedSlot[:ind], reservedSlot[ind+1:]...)
			break
		}
	}
	if err != nil {
		return err
	}
	return nil
}

var reservedSlot []int

func GetAEmptySlot(mutex *sync.Mutex) (int, error) {
	mutex.Lock()
	defer mutex.Unlock()
	statusString, err := GetMTXStatus()
	if err != nil {
		return -1, err
	}
	matches := emptyReg.FindAllStringSubmatch(statusString, -1)
	if matches == nil {
		return -1, err
	}
	for _, str := range matches {
		slot, err := strconv.Atoi(str[1])
		if err != nil {
			return -1, err
		}
		isReserved := false
		for _, val := range reservedSlot {
			if slot == val {
				isReserved = true
				break
			}
		}
		if !isReserved {
			return slot, nil
		}
	}

	return -1, nil
}

package tape

import (
	"bytes"
	"os/exec"
	"strconv"
)

// Unload Method is used to unload a tape from drive "driveNum", and place the tape in slotnumber "slotNum"
func Unload(driveNum int, slotNum int) error {
	driveN := strconv.Itoa(driveNum)
	slotN := strconv.Itoa(slotNum)
	cmd := exec.Command("mtx", "-f", "/dev/sg10", "unload", slotN, driveN)
	var errorMessg bytes.Buffer
	cmd.Stderr = &errorMessg
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
	var errorMessg bytes.Buffer
	cmd.Stderr = &errorMessg
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
	var errorMessg bytes.Buffer
	cmd.Stderr = &errorMessg
	err := cmd.Run()
	if err != nil {
		return err
	}
	return nil
}

func GetAEmptySlot() (int, error) {
	statusString, err := GetMTXStatus()
	if err != nil {
		return -1, err
	}
	matches := emptyReg.FindStringSubmatch(statusString)
	if matches == nil {
		return -1, err
	}

	slot, err := strconv.Atoi(matches[1])
	if err != nil {
		return -1, err
	}

	return slot, nil

}

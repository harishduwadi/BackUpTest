package tape

import (
	"bytes"
	"errors"
	"os/exec"
	"strconv"
)

// Unload Method is used to unload a tape from drive "driveNum", and place the tape in slotnumber "slotNum"
func Unload(driveNum int, slotNum int) error {
	driveN := strconv.Itoa(driveNum)
	slotN := strconv.Itoa(slotNum)
	cmd := exec.Command("mtx", "-f", "/dev/sg4", "unload", slotN, driveN)
	var errorMessg bytes.Buffer
	cmd.Stderr = &errorMessg
	err := cmd.Run()
	if err != nil {
		return errors.New(errorMessg.String())
	}
	return nil
}

// GetStatus is used to get the status of the tape library, which slot are empty, full, and name of tape if present
func GetStatus(command string, path string) (string, error) {

	cmd := exec.Command(command, "-f", path, "status")
	var out bytes.Buffer
	cmd.Stdout = &out
	var errorMessg bytes.Buffer
	cmd.Stderr = &errorMessg
	err := cmd.Run()
	if err != nil {
		return "", errors.New(errorMessg.String())
	}

	return out.String(), nil
}

// TODO: need to find how does the tape drive get paired with particular file

// Load is used to load a tape form slot number "SlotNum" to drive "driveNum"
func Load(driveNum int, slotNum int) error {
	driveN := strconv.Itoa(driveNum)
	slotN := strconv.Itoa(slotNum)
	cmd := exec.Command("mtx", "-f", "/dev/sg4", "load", slotN, driveN)
	var errorMessg bytes.Buffer
	cmd.Stderr = &errorMessg
	err := cmd.Run()
	if err != nil {
		return errors.New(errorMessg.String())
	}
	return nil
}

// GetAEmptySlot is used to find a empty slot in the tape library
func GetAEmptySlot() (int, error) {
	statusString, err := GetStatus("mtx", "/dev/sg4")
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

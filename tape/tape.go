package tape

import (
	"bufio"
	"fmt"
	"os"
	"regexp"

	"github.com/benmcclelland/mtio"
)

type Config struct {
	TapePath        string
	RecordSize      int
	Tape            *os.File
	TapeWriter      *bufio.Writer
	lowerTapeBuffer *bufio.Writer
}

var emptyReg *regexp.Regexp
var fullReg *regexp.Regexp

func init() {
	emptyExp := "Storage.*\\s(\\d+):Empty.*"
	fullExp := "Storage.*\\s(\\d+):Full.*=(.*)"
	var err error
	emptyReg, err = regexp.Compile(emptyExp)
	if err != nil {
		return
	}
	fullReg, err = regexp.Compile(fullExp)
	if err != nil {
		return
	}
}

func New(tapePath string, recordSize int) (*Config, error) {
	tape, err := os.OpenFile(tapePath, os.O_RDWR, os.ModePerm)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return nil, err
	}

	// This is the low level writer that will directly write to the tape at block size of 4096
	lowerTapeBuffer := bufio.NewWriterSize(tape, recordSize)

	// This is the buffer that will be used to maintain structure of tar while writing to tape
	tapeWriter := bufio.NewWriter(lowerTapeBuffer)

	return &Config{
		TapePath:        tapePath,
		RecordSize:      recordSize,
		Tape:            tape,
		TapeWriter:      tapeWriter,
		lowerTapeBuffer: lowerTapeBuffer,
	}, nil
}

func (ConfigVar *Config) DeepCopy(tapePath string) error {
	temp, err := New(tapePath, ConfigVar.RecordSize)
	if err != nil {
		return err
	}
	ConfigVar.lowerTapeBuffer = temp.lowerTapeBuffer
	ConfigVar.Tape = temp.Tape
	ConfigVar.TapeWriter = temp.TapeWriter
	ConfigVar.TapePath = temp.TapePath

	return nil
}

func (ConfigVar *Config) FlushBuffers() error {
	if err := ConfigVar.TapeWriter.Flush(); err != nil {
		return err
	}
	return ConfigVar.lowerTapeBuffer.Flush()
}

func (ConfigVar *Config) CloseTape() error {
	return ConfigVar.Tape.Close()
}

func (ConfigVar *Config) WriteEOF() error {
	return mtio.DoOp(ConfigVar.Tape, mtio.NewMtOp(mtio.WithOperation(mtio.MTWEOF)))
}

func (ConfigVar *Config) JumpToEOM() error {
	return mtio.DoOp(ConfigVar.Tape, mtio.NewMtOp(mtio.WithOperation(mtio.MTEOM)))
}

func (ConfigVar *Config) GetFileMarkNum() int {
	struc, _ := mtio.GetStatus(ConfigVar.Tape)
	return int(struc.FileNo)
}

func (ConfigVar *Config) RetensionOfTape() error {
	return mtio.DoOp(ConfigVar.Tape, mtio.NewMtOp(mtio.WithOperation(mtio.MTRETEN)))
}

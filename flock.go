package peach

import (
	"os"
	"syscall"
)

type FileLock struct {
	file *os.File
	path string
}

func NewFlock(path string) *FileLock {
	return &FileLock{path: path}
}

func (fl *FileLock) open() error {
	f, err := os.OpenFile(fl.path, os.O_CREATE|os.O_RDONLY, os.ModePerm)
	if err != nil {
		return err
	}
	fl.file = f
	return nil
}

func (fl *FileLock) TryLock() error {
	if err := fl.open(); err != nil {
		return err
	}

	if err := syscall.Flock(int(fl.file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		fl.file.Close()
		return err
	}
	return nil
}

func (fl *FileLock) ULock() error {
	if err := syscall.Flock(int(fl.file.Fd()), syscall.LOCK_UN|syscall.LOCK_NB); err != nil {
		return nil
	}
	return fl.file.Close()
}

package main

import (
	"fmt"
	"sync"
)

type threadSafeBackupErrorSlice struct {
	sync.Mutex
	value []backupError
}

type backupError struct {
	objectName string
	err        error
}

func (s *threadSafeBackupErrorSlice) append(e backupError) {
	s.Lock()
	s.value = append(s.value, e)
	s.Unlock()
}

func (s *threadSafeBackupErrorSlice) len() int {
	s.Lock()
	l := len(s.value)
	s.Unlock()
	return l
}

func (s *threadSafeBackupErrorSlice) getFormattedErrors() string {
	formatted := ""
	s.Lock()

	for _, err := range s.value {
		formatted += fmt.Sprintf("%s: %v\n", err.objectName, err.err)
	}

	s.Unlock()
	return formatted
}

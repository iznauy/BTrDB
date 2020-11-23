package log

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

const logFile = "/data/log/brain.log"

var (
	logger *Logger
)

func init() {
	logger = newLogger()
}

type Logger struct {
	buffer []string
	fd     *os.File
	mu     sync.Mutex
}

func newLogger() *Logger {
	l := &Logger{
		buffer: make([]string, 0, 10),
	}
	fd, err := os.OpenFile(logFile, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	l.fd = fd
	l.daemon()
	return l
}

func (l *Logger) Info(format string, a ...interface{}) {
	now := time.Now().Unix()
	content := fmt.Sprintf(format, a)
	l.mu.Lock()
	l.buffer = append(l.buffer, fmt.Sprintf("%d %s", now, content))
	l.mu.Unlock()
}

func GetLogger() *Logger {
	return logger
}

func (l *Logger) daemon() {
	for {
		time.Sleep(5 * time.Second)
		l.mu.Lock()
		buffer := l.buffer
		l.buffer = make([]string, 0, 10)
		l.mu.Unlock()
		if len(buffer) == 0 {
			continue
		}
		if _, err := l.fd.Write([]byte(strings.Join(buffer, "\n"))); err != nil {
			panic(err)
		}
		if err := l.fd.Sync(); err != nil {
			panic(err)
		}
	}
}

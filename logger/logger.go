package logger

import (
	"io"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	logFileMode        = os.O_CREATE | os.O_APPEND | os.O_WRONLY
	logFilePermissions = 0666
)

// Config represents a logger config.
type Config struct {
	Level    string `json:"level"`
	ToStdout bool   `json:"to_stdout"`
	ToFile   bool   `json:"to_file"`
	FilePath string `json:"file_path"`
}

// Logger represents a logger instance.
type Logger struct {
	*logrus.Logger
	config *Config
	file   *os.File
}

// New returns a new logger instance.
func New(config *Config) *Logger {
	logger := logrus.New()

	level, err := logrus.ParseLevel(config.Level)
	if err != nil {
		logger.Warnf("Could not parse log level, setting info")
		level = logrus.InfoLevel
	}

	logger.SetLevel(level)

	logOutputList := make([]io.Writer, 0)

	if config.ToStdout {
		logOutputList = append(logOutputList, os.Stdout)
	}

	var file *os.File
	if config.ToFile {
		file, err = os.OpenFile(config.FilePath, logFileMode, logFilePermissions)
		if err != nil {
			logger.Warnf("Could not open log file: %v", err)
		} else {
			logOutputList = append(logOutputList, file)
		}
	}

	logger.SetOutput(io.MultiWriter(logOutputList...))

	return &Logger{
		Logger: logger,
		config: config,
		file:   file,
	}
}

// Close closes the logger instance and the log file if it presents.
func (l *Logger) Close() error {
	if l.file == nil {
		return nil
	}

	if err := l.file.Close(); err != nil {
		return errors.Wrapf(err, "could not close log file")
	}

	return nil
}

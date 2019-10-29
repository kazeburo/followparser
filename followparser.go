package followparser

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// initialBufSize for bufio
var initialBufSize = 10000

// maxBufSize for bufio default 65537
var maxBufSize = 5 * 1000 * 1000

// MaxReadSize : Maximum size for read
var MaxReadSize int64 = 500 * 1000 * 1000

type callback interface {
	Parse(b []byte) error
	Finish(duration float64)
}

func parseLog(bs *bufio.Scanner, cb callback, logger *zap.Logger) error {
	for bs.Scan() {
		b := bs.Bytes()
		err := cb.Parse(b)
		if err != nil {
			logger.Warn("Failed to parse log", zap.Error(err))
		}
		return nil
	}
	if bs.Err() != nil {
		return bs.Err()
	}
	return io.EOF
}

func parseFile(logFile string, lastPos int64, posFile string, cb callback, logger *zap.Logger) error {
	stat, err := os.Stat(logFile)
	if err != nil {
		return errors.Wrap(err, "failed to stat log file")
	}

	fstat, err := fileStat(stat)
	if err != nil {
		return errors.Wrap(err, "failed to inode of log file")
	}

	logger.Info("Analysis start",
		zap.String("logFile", logFile),
		zap.Int64("lastPos", lastPos),
		zap.Int64("Size", stat.Size()),
	)

	if lastPos == 0 && stat.Size() > MaxReadSize {
		// first time and big logfile
		lastPos = stat.Size()
	}

	if stat.Size()-lastPos > MaxReadSize {
		// big delay
		lastPos = stat.Size()
	}

	f, err := os.Open(logFile)
	if err != nil {
		return errors.Wrap(err, "failed to open log file")
	}
	defer f.Close()
	fpr, err := NewReader(f, lastPos)
	if err != nil {
		return errors.Wrap(err, "failed to seek log file")
	}

	total := 0
	bs := bufio.NewScanner(fpr)
	bs.Buffer(make([]byte, initialBufSize), maxBufSize)
	for {
		e := parseLog(bs, cb, logger)
		if e == io.EOF {
			break
		}
		if e != nil {
			return errors.Wrap(e, "Something wrong in parse log")
		}
		total++
	}

	logger.Info("Analysis completed",
		zap.String("logFile", logFile),
		zap.Int64("startPos", lastPos),
		zap.Int64("endPos", fpr.Pos),
		zap.Int("Rows", total),
	)
	// update postion
	if posFile != "" {
		err = writePos(posFile, fpr.Pos, fstat)
		if err != nil {
			return errors.Wrap(err, "failed to update pos file")
		}
	}
	return nil
}

// Parse : parse logfile
func Parse(posFileName, logFile string, cb callback, logger *zap.Logger) error {
	lastPos := int64(0)
	lastFstat := &fStat{}
	tmpDir := os.TempDir()
	curUser, _ := user.Current()
	uid := "0"
	if curUser != nil {
		uid = curUser.Uid
	}
	posFile := filepath.Join(tmpDir, fmt.Sprintf("%s-%s", posFileName, uid))
	duration := float64(0)

	if fileExists(posFile) {
		l, d, f, err := readPos(posFile)
		if err != nil {
			return errors.Wrap(err, "failed to load pos file")
		}
		lastPos = l
		duration = d
		lastFstat = f
	}
	stat, err := os.Stat(logFile)
	if err != nil {
		return errors.Wrap(err, "failed to stat log file")
	}
	fstat, err := fileStat(stat)
	if err != nil {
		return errors.Wrap(err, "failed to get inode from log file")
	}
	if fstat.IsNotRotated(lastFstat) {
		err := parseFile(
			logFile,
			lastPos,
			posFile,
			cb,
			logger,
		)
		if err != nil {
			return err
		}
	} else {
		// rotate!!
		logger.Info("Detect Rotate")
		lastFile, err := searchFileByInode(filepath.Dir(logFile), lastFstat)
		if err != nil {
			logger.Warn("Could not search previous file",
				zap.Error(err),
			)
			// new file
			err := parseFile(
				logFile,
				0, // lastPos
				posFile,
				cb,
				logger,
			)
			if err != nil {
				return err
			}
		} else {
			// new file
			err := parseFile(
				logFile,
				0, // lastPos
				posFile,
				cb,
				logger,
			)
			if err != nil {
				return err
			}
			// previous file
			err = parseFile(
				lastFile,
				lastPos,
				"", // no update posfile
				cb,
				logger,
			)
			if err != nil {
				logger.Warn("Could not parse previous file",
					zap.Error(err),
				)
			}
		}
	}

	cb.Finish(duration)

	return nil
}

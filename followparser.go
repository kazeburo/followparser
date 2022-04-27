package followparser

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"os/user"
	"path/filepath"
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

func parseLog(bs *bufio.Scanner, cb callback) error {
	for bs.Scan() {
		b := bs.Bytes()
		err := cb.Parse(b)
		if err != nil {
			log.Printf("Failed to parse log :%v", err)
		}
		return nil
	}
	if bs.Err() != nil {
		return bs.Err()
	}
	return io.EOF
}

func parseFile(logFile string, lastPos int64, posFile string, cb callback) error {
	stat, err := os.Stat(logFile)
	if err != nil {
		return fmt.Errorf("failed to stat log file :%v", err)
	}

	fstat, err := fileStat(stat)
	if err != nil {
		return fmt.Errorf("failed to inode of log file: %v", err)
	}

	log.Printf("Analysis start logFile:%s lastPos:%d Size:%d", logFile, lastPos, stat.Size())

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
		return fmt.Errorf("failed to open log file :%v", err)
	}
	defer f.Close()
	fpr, err := NewReader(f, lastPos)
	if err != nil {
		return fmt.Errorf("failed to seek log file :%v", err)
	}

	total := 0
	bs := bufio.NewScanner(fpr)
	bs.Buffer(make([]byte, initialBufSize), maxBufSize)
	for {
		e := parseLog(bs, cb)
		if e == io.EOF {
			break
		}
		if e != nil {
			return fmt.Errorf("something wrong in parse log :%v", e)
		}
		total++
	}

	log.Printf("Analysis completed logFile:%s startPos:%d endPos:%d Rows:%d", logFile, lastPos, fpr.Pos, total)

	// update postion
	if posFile != "" {
		err = writePos(posFile, fpr.Pos, fstat)
		if err != nil {
			return fmt.Errorf("failed to update pos file :%v", err)
		}
	}
	return nil
}

// Parse : parse logfile
func Parse(posFileName, logFile string, cb callback) error {
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
			return fmt.Errorf("failed to load pos file :%v", err)
		}
		lastPos = l
		duration = d
		lastFstat = f
	}
	stat, err := os.Stat(logFile)
	if err != nil {
		return fmt.Errorf("failed to stat log file :%v", err)
	}
	fstat, err := fileStat(stat)
	if err != nil {
		return fmt.Errorf("failed to get inode from log file :%v", err)
	}
	if fstat.IsNotRotated(lastFstat) {
		err := parseFile(
			logFile,
			lastPos,
			posFile,
			cb,
		)
		if err != nil {
			return err
		}
	} else {
		// rotate!!
		log.Printf("Detect Rotate")
		lastFile, err := searchFileByInode(filepath.Dir(logFile), lastFstat)
		if err != nil {
			log.Printf("Could not search previous file :%v", err)
			// new file
			err := parseFile(
				logFile,
				0, // lastPos
				posFile,
				cb,
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
			)
			if err != nil {
				log.Printf("Could not parse previous file :%v", err)
			}
		}
	}

	cb.Finish(duration)

	return nil
}

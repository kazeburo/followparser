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

// DefaultMaxReadSize : Maximum size for read
var DefaultMaxReadSize int64 = 500 * 1000 * 1000

type Callback interface {
	Parse(b []byte) error
	Finish(duration float64)
}

type Parser struct {
	maxReadSize int64
}

func New(s int64) *Parser {
	return &Parser{maxReadSize: s}
}

func Parse(posFileName, logFile string, cb Callback) error {
	parser := New(DefaultMaxReadSize)
	return parser.parse(posFileName, logFile, cb)
}

func (parser *Parser) parse(posFileName, logFile string, cb Callback) error {
	tmpDir := os.TempDir()
	curUser, _ := user.Current()
	uid := "0"
	if curUser != nil {
		uid = curUser.Uid
	}

	pf := newPosFile(filepath.Join(tmpDir, fmt.Sprintf("%s-%s", posFileName, uid)))
	lastPos, duration, lastFstat, err := pf.read()
	if err != nil {
		return fmt.Errorf("failed to load pos file :%v", err)
	}

	fstat, err := fileStat(logFile)
	if err != nil {
		return fmt.Errorf("failed to get inode from log file :%v", err)
	}
	// return fmt.Errorf("%v", lastFstat)
	if fstat.IsNotRotated(lastFstat) {
		err := parser.parseFile(
			logFile,
			lastPos,
			pf,
			cb,
		)
		if err != nil {
			return err
		}
	} else {
		// rotate found
		log.Printf("Detect Rotate")
		lastFile, err := searchFileByInode(filepath.Dir(logFile), lastFstat)
		if err != nil {
			log.Printf("Could not search previous file :%v", err)
			// new file only
			err := parser.parseFile(
				logFile,
				0, // lastPos
				pf,
				cb,
			)
			if err != nil {
				return err
			}
		} else {
			// new file
			err := parser.parseFile(
				logFile,
				0, // lastPos
				pf,
				cb,
			)
			if err != nil {
				return err
			}
			// previous file
			err = parser.parseFile(
				lastFile,
				lastPos,
				nil, // no update posfile
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

func (parser *Parser) parseFile(logFile string, lastPos int64, pf *posFile, cb Callback) error {

	fstat, err := fileStat(logFile)
	if err != nil {
		return fmt.Errorf("failed to inode of log file: %v", err)
	}

	log.Printf("Analysis start logFile:%s lastPos:%d Size:%d", logFile, lastPos, fstat.Size)

	if lastPos == 0 && fstat.Size > parser.maxReadSize {
		// first time and big logfile
		lastPos = fstat.Size
	}

	if fstat.Size-lastPos > parser.maxReadSize {
		// big delay
		lastPos = fstat.Size
	}

	f, err := os.Open(logFile)
	if err != nil {
		return fmt.Errorf("failed to open log file :%v", err)
	}
	defer f.Close()
	fpr, err := newReader(f, lastPos)
	if err != nil {
		return fmt.Errorf("failed to seek log file :%v", err)
	}

	total := 0
	bs := bufio.NewScanner(fpr)
	bs.Buffer(make([]byte, initialBufSize), maxBufSize)
	for {
		e := parser.parseLog(bs, cb)
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
	if pf != nil {
		err = pf.write(fpr.Pos, fstat)
		if err != nil {
			return fmt.Errorf("failed to update pos file :%v", err)
		}
	}
	return nil
}

func (parser *Parser) parseLog(bs *bufio.Scanner, cb Callback) error {
	for bs.Scan() {
		b := bs.Bytes()
		err := cb.Parse(b)
		if err != nil {
			log.Printf("Failed to parse log :%v", err)
		}
	}
	if bs.Err() != nil {
		return bs.Err()
	}
	return io.EOF
}

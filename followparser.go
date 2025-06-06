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

const newestLog = true
const rotatedLog = false

type Callback interface {
	Parse(b []byte) error
	Finish(duration float64)
}

type Parser struct {
	WorkDir             string
	MaxReadSize         int64
	Callback            Callback
	Silent              bool
	NoAutoCommitPosFile bool
	posFile             *posFile
	lastPos             int64
	lastfStat           *fStat
}

type Parsed struct {
	FileName string
	Size     int64
	StartPos int64
	EndPos   int64
	Rows     int
}

func Parse(posFileName, logFile string, cb Callback) error {
	parser := &Parser{
		Callback: cb,
	}
	_, err := parser.Parse(posFileName, logFile)
	return err
}

func (parser *Parser) Parse(posFileName, logFile string) ([]Parsed, error) {
	if parser.WorkDir == "" {
		parser.WorkDir = os.TempDir()
	}
	if parser.MaxReadSize == 0 {
		parser.MaxReadSize = DefaultMaxReadSize
	}
	if parser.Callback == nil {
		parser.Callback = &dummyParser{}
	}
	curUser, _ := user.Current()
	uid := "0"
	if curUser != nil {
		uid = curUser.Uid
	}

	parser.posFile = newPosFile(filepath.Join(parser.WorkDir, fmt.Sprintf("%s-%s", posFileName, uid)))
	lastPos, duration, lastFstat, err := parser.posFile.read()
	if err != nil {
		return nil, fmt.Errorf("failed to load pos file :%v", err)
	}

	fstat, err := fileStat(logFile)
	if err != nil {
		return nil, fmt.Errorf("failed to get inode from log file :%v", err)
	}
	result := make([]Parsed, 0)
	if fstat.isNotRotated(lastFstat) {
		parsed, err := parser.parseFile(
			logFile,
			lastPos,
			newestLog,
		)
		if err != nil {
			return nil, err
		}
		result = append(result, *parsed)
	} else {
		// rotate found
		if !parser.Silent {
			log.Printf("Detect Rotate")
		}
		lastFile, err := lastFstat.searchFileByInode(filepath.Dir(logFile))
		if err != nil {
			log.Printf("Could not search previous file :%v", err)
			// new file only
			parsed, err := parser.parseFile(
				logFile,
				0, // lastPos
				newestLog,
			)
			if err != nil {
				return nil, err
			}
			result = append(result, *parsed)
		} else {
			// previous file
			parsed, err := parser.parseFile(
				lastFile,
				lastPos,
				rotatedLog, // no update posfile
			)
			if err != nil {
				log.Printf("Could not parse previous file :%v", err)
			}
			if parsed != nil {
				result = append(result, *parsed)
			}
			// new file
			parsed, err = parser.parseFile(
				logFile,
				0, // lastPos
				newestLog,
			)
			if err != nil {
				return nil, err
			}
			result = append(result, *parsed)
		}
	}

	parser.Callback.Finish(duration)

	return result, nil
}

func (parser *Parser) parseFile(logFile string, lastPos int64, newestLog bool) (*Parsed, error) {

	fstat, err := fileStat(logFile)
	if err != nil {
		return nil, fmt.Errorf("failed to inode of log file: %v", err)
	}
	if !parser.Silent {
		log.Printf("Analysis start logFile:%s lastPos:%d Size:%d", logFile, lastPos, fstat.Size)
	}
	if lastPos == 0 && fstat.Size > parser.MaxReadSize {
		// first time and big logfile
		lastPos = fstat.Size
	}

	if fstat.Size-lastPos > parser.MaxReadSize {
		// big delay
		lastPos = fstat.Size
	}

	f, err := os.Open(logFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file :%v", err)
	}
	defer f.Close()
	fpr, err := newReader(f, lastPos)
	if err != nil {
		return nil, fmt.Errorf("failed to seek log file :%v", err)
	}

	total := 0
	bs := bufio.NewScanner(fpr)
	bs.Buffer(make([]byte, initialBufSize), maxBufSize)
	for {
		scan, e := parser.parseLog(bs)
		total += scan
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, fmt.Errorf("something wrong in parse log :%v", e)
		}

	}

	// update postion
	if newestLog {
		parser.lastPos = fpr.Pos
		parser.lastfStat = fstat
		if !parser.NoAutoCommitPosFile {
			err = parser.posFile.write(fpr.Pos, fstat)
			if err != nil {
				return nil, fmt.Errorf("failed to update pos file :%v", err)
			}
		}
	}

	parsed := &Parsed{
		FileName: logFile,
		Size:     fstat.Size,
		StartPos: lastPos,
		EndPos:   fpr.Pos,
		Rows:     total,
	}
	if !parser.Silent {
		log.Printf("Analysis completed logFile:%s startPos:%d endPos:%d Rows:%d", logFile, lastPos, fpr.Pos, total)
	}

	return parsed, nil
}

func (parser *Parser) CommitPosFile() error {
	if parser.posFile == nil {
		return nil
	}
	err := parser.posFile.write(parser.lastPos, parser.lastfStat)
	if err != nil {
		return fmt.Errorf("failed to update pos file :%v", err)
	}
	return nil
}

func (parser *Parser) parseLog(bs *bufio.Scanner) (int, error) {
	scan := 0
	for bs.Scan() {
		b := bs.Bytes()
		err := parser.Callback.Parse(b)
		if err != nil {
			log.Printf("Failed to parse log :%v", err)
		}
		scan++
	}
	if bs.Err() != nil {
		return scan, bs.Err()
	}
	return scan, io.EOF
}

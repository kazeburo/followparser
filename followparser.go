package followparser

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/user"
	"path/filepath"
)

// initialBufSize for scanFile
var initialBufSize = 32 * 1000

// maxBufSize for scanFile
var maxBufSize = 5 * 1000 * 1000

// DefaultMaxReadSize : Maximum size for read
var DefaultMaxReadSize int64 = 500 * 1000 * 1000

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
	ArchiveDir          string
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
	// If ArchiveDir is not set, default to the directory containing the log file.
	// This fallback ensures archived logs are stored alongside the original log by default.
	if parser.ArchiveDir == "" {
		parser.ArchiveDir = filepath.Dir(logFile)
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
			true,
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
		lastFile, err := lastFstat.searchFileByInode(parser.ArchiveDir)
		if err != nil {
			log.Printf("Could not search previous file :%v", err)
			// new file only
			parsed, err := parser.parseFile(
				logFile,
				0, // lastPos
				true,
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
				false, // no update posfile
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
				true,
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

func seekToPos(f io.Reader, pos int64) error {
	if is, ok := f.(io.Seeker); ok {
		_, err := is.Seek(pos, 0)
		if err != nil {
			return err
		}
	}
	return nil
}

func (parser *Parser) parseFile(logFile string, lastPos int64, newest bool) (*Parsed, error) {

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
	err = seekToPos(f, lastPos)
	if err != nil {
		return nil, fmt.Errorf("failed to seek log file :%v", err)
	}

	rows, read, err := parser.scanFile(f, newest)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("something wrong in parse log :%v", err)
	}
	curPos := lastPos + read

	// update postion
	if newest {
		parser.lastPos = curPos
		parser.lastfStat = fstat
		if !parser.NoAutoCommitPosFile {
			err = parser.posFile.write(curPos, fstat)
			if err != nil {
				return nil, fmt.Errorf("failed to update pos file :%v", err)
			}
		}
	}

	parsed := &Parsed{
		FileName: logFile,
		Size:     fstat.Size,
		StartPos: lastPos,
		EndPos:   curPos,
		Rows:     rows,
	}
	if !parser.Silent {
		log.Printf("Analysis completed logFile:%s startPos:%d endPos:%d Rows:%d", logFile, lastPos, curPos, rows)
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

func (parser *Parser) scanFile(f io.Reader, newest bool) (int, int64, error) {
	scan := 0
	read := int64(0)
	buf := make([]byte, initialBufSize)
	offset := 0
	for {
		n, err := f.Read(buf[offset:])
		if err != nil {
			return scan, read, err
		}

		n += offset
		if bytes.IndexByte(buf[:n], '\n') < 0 {
			if n == maxBufSize {
				// buffer full
				return scan, read, errors.New("reader: token too Long")
			} else if n == len(buf) {
				// buffer full and to expand buffer
				newSize := len(buf) * 2
				newSize = min(newSize, maxBufSize)
				newBuf := make([]byte, newSize)
				copy(newBuf, buf)
				buf = newBuf
			} else if !newest {
				// no newline but not full buffer and not newest file
				// treat as end of file
				read += int64(n)
				err := parser.Callback.Parse(buf[0:n])
				if err != nil {
					log.Printf("Failed to parse log :%v", err)
				}
				scan++
				return scan, read, io.EOF

			}
			// continue to read
			offset = n
			continue
		}

		k := 0
		for i := bytes.IndexByte(buf[k:n], '\n'); i >= 0; i = bytes.IndexByte(buf[k:n], '\n') {
			read += int64(i + 1) // +1 for newline
			err := parser.Callback.Parse(buf[k : k+i])
			if err != nil {
				log.Printf("Failed to parse log :%v", err)
			}
			scan++
			k = k + i + 1
		}
		if k < n {
			// move remaining to head
			copy(buf[0:], buf[k:n])
			offset = n - k
		} else {
			offset = 0
		}
	}
}

package followparser

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// FilePos :
type filePos struct {
	Pos   int64   `json:"pos"`
	Time  float64 `json:"time"`
	Inode uint64  `json:"inode"`
	Dev   uint64  `json:"dev"`
}

// FStat :
type fStat struct {
	Inode uint64
	Dev   uint64
}

// FileExists :
func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}

// FileStat :
func fileStat(s os.FileInfo) (*fStat, error) {
	s2 := s.Sys().(*syscall.Stat_t)
	if s2 == nil {
		return &fStat{}, fmt.Errorf("Could not get Inode")
	}
	return &fStat{s2.Ino, uint64(s2.Dev)}, nil
}

// IsNotRotated :
func (fstat *fStat) IsNotRotated(lastFstat *fStat) bool {
	return lastFstat.Inode == 0 || lastFstat.Dev == 0 || (fstat.Inode == lastFstat.Inode && fstat.Dev == lastFstat.Dev)
}

// SearchFileByInode :
func searchFileByInode(d string, fstat *fStat) (string, error) {
	files, err := ioutil.ReadDir(d)
	if err != nil {
		return "", err
	}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		s, _ := fileStat(file)
		if s.Inode == fstat.Inode && s.Dev == fstat.Dev {
			return filepath.Join(d, file.Name()), nil
		}
	}
	return "", fmt.Errorf("There is no file by inode:%d in %s", fstat.Inode, d)
}

// WritePos :
func writePos(filename string, pos int64, fstat *fStat) error {
	fp := filePos{pos, float64(time.Now().Unix()), fstat.Inode, fstat.Dev}
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	jb, err := json.Marshal(fp)
	if err != nil {
		return err
	}
	_, err = file.Write(jb)
	return err
}

// ReadPos :
func readPos(filename string) (int64, float64, *fStat, error) {
	fp := filePos{}
	d, err := ioutil.ReadFile(filename)
	if err != nil {
		return 0, 0, &fStat{}, err
	}
	err = json.Unmarshal(d, &fp)
	if err != nil {
		return 0, 0, &fStat{}, err
	}
	duration := float64(time.Now().Unix()) - fp.Time
	return fp.Pos, duration, &fStat{fp.Inode, fp.Dev}, nil
}

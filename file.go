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

type fPos struct {
	Pos   int64   `json:"pos"`
	Time  float64 `json:"time"`
	Inode uint64  `json:"inode"`
	Dev   uint64  `json:"dev"`
}

type fStat struct {
	Inode uint64
	Dev   uint64
	Size  int64
}

type posFile struct {
	filename string
}

func newPosFile(filename string) *posFile {
	return &posFile{filename}
}

func (pf *posFile) read() (int64, float64, *fStat, error) {
	s, err := os.Stat(pf.filename)
	if err != nil || s.Size() == 0 {
		return 0, 0, nil, nil
	}

	d, err := ioutil.ReadFile(pf.filename)
	if err != nil {
		return 0, 0, nil, err
	}
	fp := fPos{}
	err = json.Unmarshal(d, &fp)
	if err != nil {
		return 0, 0, nil, err
	}
	duration := float64(time.Now().Unix()) - fp.Time
	return fp.Pos,
		duration,
		&fStat{
			Inode: fp.Inode,
			Dev:   fp.Dev,
			Size:  0,
		},
		nil
}

func (pf *posFile) write(pos int64, fstat *fStat) error {
	fp := fPos{
		Pos:   pos,
		Time:  float64(time.Now().Unix()),
		Inode: fstat.Inode,
		Dev:   fstat.Dev,
	}
	file, err := os.Create(pf.filename)
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

func fileStat(filename string) (*fStat, error) {
	s, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}
	s2 := s.Sys().(*syscall.Stat_t)
	if s2 == nil {
		return nil, fmt.Errorf("could not get inode")
	}
	return &fStat{
		Inode: s2.Ino,
		Dev:   uint64(s2.Dev),
		Size:  s.Size(),
	}, nil
}

// IsNotRotated :
func (fstat *fStat) IsNotRotated(lastFstat *fStat) bool {
	return lastFstat.Inode == 0 || lastFstat.Dev == 0 || (fstat.Inode == lastFstat.Inode && fstat.Dev == lastFstat.Dev)
}

func searchFileByInode(d string, fstat *fStat) (string, error) {
	files, err := ioutil.ReadDir(d)
	if err != nil {
		return "", err
	}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		s, _ := fileStat(file.Name())
		if s.Inode == fstat.Inode && s.Dev == fstat.Dev {
			return filepath.Join(d, file.Name()), nil
		}
	}
	return "", fmt.Errorf("there is no file by inode:%d in %s", fstat.Inode, d)
}

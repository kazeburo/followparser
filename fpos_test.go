package followparser

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"path"
	"testing"
	"time"
)

func init() {
	log.SetOutput(io.Discard)
}
func TestPosFileRead(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pos_file_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %s", err)
	}
	defer os.RemoveAll(tmpDir)

	tests := []struct {
		name          string
		content       *fPos
		expectedPos   int64
		expectedTime  float64
		expectedFStat *fStat
		expectedError bool
	}{
		{
			name: "valid file",
			content: &fPos{
				Pos:   123,
				Time:  float64(time.Now().Unix()),
				Inode: 1,
				Dev:   2,
			},
			expectedPos:  123,
			expectedTime: 0.0, // Ideally time should be compared within a range
			expectedFStat: &fStat{
				Inode: 1,
				Dev:   2,
				Size:  0,
			},
			expectedError: false,
		},
		{
			name:          "file does not exist",
			content:       nil,
			expectedPos:   0,
			expectedTime:  0,
			expectedFStat: nil,
			expectedError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			filename := path.Join(tmpDir, tc.name)
			if tc.content != nil {
				fileContent, _ := json.Marshal(tc.content)
				os.WriteFile(filename, fileContent, 0666)
			}

			pf := newPosFile(filename)
			pos, _, fstat, err := pf.read()

			if (err != nil) != tc.expectedError {
				t.Errorf("Expected error: %v, got: %v", tc.expectedError, err)
			}

			if pos != tc.expectedPos {
				t.Errorf("Expected pos: %d, got: %d", tc.expectedPos, pos)
			}

			if fstat != nil && tc.expectedFStat != nil &&
				(fstat.Inode != tc.expectedFStat.Inode || fstat.Dev != tc.expectedFStat.Dev) {
				t.Errorf(`Expected fstat: %+v, got: %+v`, tc.expectedFStat, fstat)
			}

			// Validate duration within a reasonable time range if needed
		})
	}
}

func TestPosFileWrite(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pos_file_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %s", err)
	}
	defer os.RemoveAll(tmpDir)

	tests := []struct {
		name          string
		pos           int64
		fstat         *fStat
		expectedError bool
	}{
		{
			name: "valid write",
			pos:  456,
			fstat: &fStat{
				Inode: 3,
				Dev:   4,
			},
			expectedError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			filename := path.Join(tmpDir, "pos_file.json")
			pf := newPosFile(filename)
			err := pf.write(tc.pos, tc.fstat)

			if (err != nil) != tc.expectedError {
				t.Errorf("Expected error: %v, got: %v", tc.expectedError, err)
			}

			if !tc.expectedError {
				content, _ := os.ReadFile(filename)
				readFPos := &fPos{}
				json.Unmarshal(content, readFPos)

				if readFPos.Pos != tc.pos {
					t.Errorf("Expected pos: %d, got: %d", tc.pos, readFPos.Pos)
				}

				if readFPos.Inode != tc.fstat.Inode || readFPos.Dev != tc.fstat.Dev {
					t.Errorf("Expected fstat: %+v, got: %+v", tc.fstat, readFPos)
				}

				// Validate time within a reasonable range if necessary
			}
		})
	}
}

func TestPosFileConcurrentReadAndWrite(t *testing.T) {
	t.Parallel()
	tmpDir, err := os.MkdirTemp("", "pos_file_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %s", err)
	}
	defer os.RemoveAll(tmpDir)

	filename := path.Join(tmpDir, "pos_file.json")
	pf := newPosFile(filename)

	initialPos := int64(789)
	initialFStat := &fStat{
		Inode: 5,
		Dev:   6,
	}
	err = pf.write(initialPos, initialFStat)
	if err != nil {
		t.Fatalf("Failed to write initial data: %s", err)
	}

	iterations := 100
	done := make(chan struct{})
	go func() {
		for i := 0; i < iterations; i++ {
			_, _, _, err := pf.read()
			if err != nil {
				t.Errorf("read operation failed: %s", err)
			}
			time.Sleep(time.Millisecond)
		}
		close(done)
	}()

	for i := 0; i < iterations; i++ {
		pos := int64(1000 + i)
		err := pf.write(pos, initialFStat)
		if err != nil {
			t.Errorf("write operation failed: %s", err)
		}
		time.Sleep(time.Millisecond)
	}

	<-done
}

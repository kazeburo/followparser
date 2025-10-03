package followparser

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type testParser struct {
	buf      *bytes.Buffer
	duration float64
}

func (p *testParser) Parse(b []byte) error {
	p.buf.Write(b)
	p.buf.WriteString("\n")
	return nil
}

func (p *testParser) Finish(duration float64) {
	p.duration = duration
}

func (p *testParser) Slurp() *bytes.Buffer {
	return p.buf
}

func TestParse(t *testing.T) {
	tmpdir := t.TempDir()
	logFileName := filepath.Join(tmpdir, "log")
	fh, err := os.Create(logFileName)
	if err != nil {
		t.Error(err)
	}
	for i := 0; i < 2; i++ {
		buf := bytes.NewBufferString("")
		parser := &testParser{
			buf:      buf,
			duration: 0,
		}
		msg := fmt.Sprintf("msg msg %08d\n", i)
		fh.WriteString(msg)
		fp := &Parser{
			WorkDir:  tmpdir,
			Callback: parser,
		}
		r, err := fp.Parse("logPos", logFileName)
		if err != nil {
			t.Error(err)
		}
		out := parser.Slurp().String()
		if out != msg {
			t.Errorf("read '%s' not match expect '%s'", out, msg)
		}
		if len(r) != 1 {
			t.Errorf("result len must be 1 %v", r)
		}
		if r[0].Rows != 1 {
			t.Errorf("result[0].Rows must be 1 %v", r)
		}
		if r[0].EndPos-r[0].StartPos != 17 {
			t.Errorf("r[0].EndPos - r[0].StartPos must be 17 %v", r[0])
		}
	}

	time.Sleep(time.Second)
	msg3 := fmt.Sprintf("msg msg %08d\n", 3)
	fh.WriteString(msg3)
	fh.Close()
	os.Rename(logFileName, filepath.Join(tmpdir, "log.1"))
	fh, err = os.Create(logFileName)
	if err != nil {
		t.Error(err)
	}
	msg4 := fmt.Sprintf("msg msg %08d\n", 4)
	fh.WriteString(msg4)
	buf := bytes.NewBufferString("")
	parser := &testParser{
		buf:      buf,
		duration: 0,
	}
	fp := &Parser{
		WorkDir:  tmpdir,
		Callback: parser,
		Silent:   true,
	}
	r, err := fp.Parse("logPos", logFileName)
	if err != nil {
		t.Error(err)
	}
	out := parser.Slurp().String()
	if out != msg3+msg4 {
		t.Errorf("read '%s' not match expect '%s'", out, msg3+msg4)
	}
	if parser.duration < 1 {
		t.Errorf("duration: %f", parser.duration)
	}
	if len(r) != 2 {
		t.Errorf("result len must be 2 %v", r)
	}
	if r[0].Rows != 1 {
		t.Errorf("result[0].Rows must be 1 %v", r)
	}
	if r[1].Rows != 1 {
		t.Errorf("result[1].Rows must be 1 %v", r)
	}

	// --- Archive directory move test starts here ---
	archiveDir := filepath.Join(tmpdir, "archive")
	err = os.Mkdir(archiveDir, 0755)
	if err != nil {
		t.Error(err)
	}
	// Append to the log file
	fh.Close()
	fh, err = os.OpenFile(logFileName, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Error(err)
	}
	msg5 := fmt.Sprintf("msg msg %08d\n", 5)
	fh.WriteString(msg5)
	fh.Close()
	// Move log file to archive directory
	archivedLog := filepath.Join(archiveDir, "log.2")
	err = os.Rename(logFileName, archivedLog)
	if err != nil {
		t.Error(err)
	}
	// Create a new log file and write to it
	fh, err = os.Create(logFileName)
	if err != nil {
		t.Error(err)
	}
	msg6 := fmt.Sprintf("msg msg %08d\n", 6)
	fh.WriteString(msg6)
	fh.Close()
	buf = bytes.NewBufferString("")
	parser = &testParser{
		buf:      buf,
		duration: 0,
	}
	fp = &Parser{
		WorkDir:    tmpdir,
		Callback:   parser,
		ArchiveDir: archiveDir,
		Silent:     true,
	}
	r, err = fp.Parse("logPos", logFileName)
	if err != nil {
		t.Error(err)
	}
	out = parser.Slurp().String()
	if out != msg5+msg6 {
		t.Errorf("archive follow read '%s' not match expect '%s'", out, msg5+msg6)
	}
	if len(r) != 2 {
		t.Errorf("archive follow result len must be 2 %v", r)
	}
	if r[0].Rows != 1 {
		t.Errorf("archive follow result[0].Rows must be 1 %v", r[0].Rows)
	}
	if r[1].Rows != 1 {
		t.Errorf("archive follow result[1].Rows must be 1 %v", r[1].Rows)
	}
}

func TestParseWithNoCommitPosFile(t *testing.T) {
	tmpdir := t.TempDir()
	logFileName := filepath.Join(tmpdir, "log")
	fh, err := os.Create(logFileName)
	if err != nil {
		t.Error(err)
	}

	lastmsg := ""
	var fp *Parser
	for i := 0; i < 2; i++ {
		buf := bytes.NewBufferString("")
		parser := &testParser{
			buf:      buf,
			duration: 0,
		}
		msg := fmt.Sprintf("msg msg %08d\n", i)
		lastmsg += msg
		fh.WriteString(msg)
		fp = &Parser{
			WorkDir:             tmpdir,
			Callback:            parser,
			NoAutoCommitPosFile: true,
		}
		r, err := fp.Parse("logPos", logFileName)
		if err != nil {
			t.Error(err)
		}
		out := parser.Slurp().String()
		if out != lastmsg {
			t.Errorf("read '%s' not match expect '%s'", out, lastmsg)
		}
		if len(r) != 1 {
			t.Errorf("result len must be 1 %v", len(r))
		}
		if r[0].Rows != i+1 {
			t.Errorf("result[0].Rows must be i+1 %v i=%d", r[0].Rows, i)
		}
		if r[0].EndPos-r[0].StartPos != int64(17*(i+1)) {
			t.Errorf("r[0].EndPos - r[0].StartPos must be 17*(i+1) %v i=%d", r[0].EndPos-r[0].StartPos, i)
		}
	}
	errCommit := fp.CommitPosFile()
	if errCommit != nil {
		t.Error(errCommit)
	}
	{
		buf := bytes.NewBufferString("")
		parser := &testParser{
			buf:      buf,
			duration: 0,
		}
		msg3 := fmt.Sprintf("msg msg %08d\n", 3)
		fh.WriteString(msg3)
		fp = &Parser{
			WorkDir:             tmpdir,
			Callback:            parser,
			NoAutoCommitPosFile: false,
		}
		r, err := fp.Parse("logPos", logFileName)
		if err != nil {
			t.Error(err)
		}
		out := parser.Slurp().String()
		if out != msg3 {
			t.Errorf("read '%s' not match expect '%s'", out, msg3)
		}
		if len(r) != 1 {
			t.Errorf("result len must be 1 %v", len(r))
		}
		if r[0].Rows != 1 {
			t.Errorf("result[0].Rows must be 1 %v", r[0].Rows)
		}
		if r[0].EndPos-r[0].StartPos != 17 {
			t.Errorf("r[0].EndPos - r[0].StartPos must be 17 %v", r[0].EndPos-r[0].StartPos)
		}

	}
}

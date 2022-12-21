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
	os.Setenv("TMPDIR", tmpdir)
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
		err := Parse("logPos", logFileName, parser)
		if err != nil {
			t.Error(err)
		}
		out := parser.Slurp().String()
		if out != msg {
			t.Errorf("read '%s' not match expect '%s'", out, msg)
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
	err = Parse("logPos", logFileName, parser)
	if err != nil {
		t.Error(err)
	}
	out := parser.Slurp().String()
	if out != msg4+msg3 {
		t.Errorf("read '%s' not match expect '%s'", out, msg4+msg3)
	}
	if parser.duration < 1 {
		t.Errorf("duration: %f", parser.duration)
	}

}

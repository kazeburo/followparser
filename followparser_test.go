package followparser

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type testParser struct {
	buf      *bytes.Buffer
	duration float64
}

// Helper to write a test file with repeated lines
func writeTestFile(path string, line string, count int) error {
	fh, err := os.Create(path)
	if err != nil {
		return err
	}
	defer fh.Close()
	for i := 0; i < count; i++ {
		if _, err := fh.WriteString(line); err != nil {
			return err
		}
	}
	return fh.Sync()
}

func benchScannerFile(b *testing.B, fname string) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		fh, err := os.Open(fname)
		if err != nil {
			b.Fatal(err)
		}
		parser := &testParser{buf: bytes.NewBufferString("")}
		scanner := bufio.NewScanner(fh)
		scanner.Buffer(make([]byte, initialBufSize), maxBufSize)
		for scanner.Scan() {
			if err := parser.Parse(scanner.Bytes()); err != nil {
				b.Fatal(err)
			}
		}
		if err := scanner.Err(); err != nil {
			b.Fatal(err)
		}
		fh.Close()
	}
}

func benchScanFile(b *testing.B, fname string) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		fh, err := os.Open(fname)
		if err != nil {
			b.Fatal(err)
		}
		parser := &testParser{buf: bytes.NewBufferString("")}
		p := &Parser{Callback: parser}
		_, _, err = p.scanFile(fh, true)
		if err != nil && err != io.EOF {
			b.Fatal(err)
		}
		fh.Close()
	}
}

func BenchmarkScanner_SmallLines(b *testing.B) {
	dir := b.TempDir()
	fname := filepath.Join(dir, "small.log")
	line := "short line example\n"
	// ~10k lines
	if err := writeTestFile(fname, line, 10000); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	benchScannerFile(b, fname)
}

func BenchmarkScanFile_SmallLines(b *testing.B) {
	dir := b.TempDir()
	fname := filepath.Join(dir, "small.log")
	line := "short line example\n"
	if err := writeTestFile(fname, line, 10000); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	benchScanFile(b, fname)
}

func BenchmarkScanner_LongLine(b *testing.B) {
	dir := b.TempDir()
	fname := filepath.Join(dir, "long.log")
	longLine := string(bytes.Repeat([]byte("A"), initialBufSize+100)) + "\n"
	// single long line
	if err := writeTestFile(fname, longLine, 1); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	benchScannerFile(b, fname)
}

func BenchmarkScanFile_LongLine(b *testing.B) {
	dir := b.TempDir()
	fname := filepath.Join(dir, "long.log")
	longLine := string(bytes.Repeat([]byte("A"), initialBufSize+100)) + "\n"
	if err := writeTestFile(fname, longLine, 1); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	benchScanFile(b, fname)
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

// Test when the last line in the log file does not end with a newline,
// then the file is appended to. Ensure appended content is followed.
func TestParseAppendAfterNoTrailingNewline(t *testing.T) {
	tmpdir := t.TempDir()
	logFileName := filepath.Join(tmpdir, "log")
	fh, err := os.Create(logFileName)
	if err != nil {
		t.Error(err)
	}

	// write a line without a trailing newline
	previousMsg := fmt.Sprintf("msg msg %08d\n", 7)
	previousMsg += fmt.Sprintf("msg msg %08d\n", 8)
	previousMsg += fmt.Sprintf("msg msg %08d\n", 9)
	msgNoNLBefore := "msg "
	_, err = fh.WriteString(previousMsg + msgNoNLBefore)
	if err != nil {
		t.Error(err)
	}
	fh.Sync()

	// First parse: should read the existing line (even without newline)
	buf := bytes.NewBufferString("")
	parser := &testParser{buf: buf}
	fp := &Parser{
		WorkDir:  tmpdir,
		Callback: parser,
		Silent:   true,
	}
	r, err := fp.Parse("logPosNoNL", logFileName)
	if err != nil {
		t.Error(err)
	}
	out := parser.Slurp().String()
	if out != previousMsg {
		t.Fatalf("first read '%s' not match expect '%s'", out, previousMsg)
	}
	if len(r) != 1 {
		t.Fatalf("first result len must be 1 %v", r)
	}

	// Append new content (with newline) to the same file
	fh, err = os.OpenFile(logFileName, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Error(err)
	}
	msgNoNLAfter := fmt.Sprintf("msg %08d\n", 10)
	msgAppend := fmt.Sprintf("msg msg %08d\n", 11)
	_, err = fh.WriteString(msgNoNLAfter + msgAppend)
	if err != nil {
		t.Error(err)
	}
	fh.Close()

	// Second parse using same pos file name: should read only appended content
	buf2 := bytes.NewBufferString("")
	parser2 := &testParser{buf: buf2}
	fp2 := &Parser{
		WorkDir:  tmpdir,
		Callback: parser2,
		Silent:   true,
	}
	r2, err := fp2.Parse("logPosNoNL", logFileName)
	if err != nil {
		t.Error(err)
	}
	out2 := parser2.Slurp().String()
	if out2 != msgNoNLBefore+msgNoNLAfter+msgAppend {
		t.Fatalf("second read '%s' not match expect '%s'", out2, msgNoNLBefore+msgNoNLAfter+msgAppend)
	}
	if len(r2) != 1 {
		t.Fatalf("second result len must be 1 %v", r2)
	}
}

// Test a single line longer than initialBufSize is read properly
func TestParseSingleLongLine(t *testing.T) {
	tmpdir := t.TempDir()
	logFileName := filepath.Join(tmpdir, "log")
	fh, err := os.Create(logFileName)
	if err != nil {
		t.Error(err)
	}
	// create a single long line > initialBufSize
	longLen := initialBufSize + 100
	data := bytes.Repeat([]byte("A"), longLen)
	// ensure newline at end so Scanner treats it as a line
	_, err = fh.Write(data)
	if err != nil {
		t.Error(err)
	}
	fh.WriteString("\n")
	fh.Close()

	buf := bytes.NewBufferString("")
	parser := &testParser{buf: buf}
	fp := &Parser{
		WorkDir:  tmpdir,
		Callback: parser,
		Silent:   true,
	}
	r, err := fp.Parse("logPosLong", logFileName)
	if err != nil {
		t.Error(err)
	}
	out := parser.Slurp().String()
	expected := string(data) + "\n"
	if out != expected {
		t.Fatalf("read length %d not match expect %d", len(out), len(expected))
	}
	if len(r) != 1 {
		t.Fatalf("result len must be 1 %v", r)
	}
	if r[0].Rows != 1 {
		t.Fatalf("result[0].Rows must be 1 %v", r[0].Rows)
	}
	if r[0].EndPos-r[0].StartPos != int64(len(expected)) {
		t.Fatalf("r[0].EndPos - r[0].StartPos must be %d %v", len(expected), r[0])
	}
}

// Test rotate: old (archived) file's last line has no trailing newline
// and should still be read after rotation.
func TestRotateReadOldFileWithNoTrailingNewline(t *testing.T) {
	tmpdir := t.TempDir()
	logFileName := filepath.Join(tmpdir, "log")
	fh, err := os.Create(logFileName)
	if err != nil {
		t.Error(err)
	}
	// write first line with newline and parse to set pos file
	msg1 := fmt.Sprintf("msg msg %08d\n", 30)
	fh.WriteString(msg1)
	fh.Sync()

	buf := bytes.NewBufferString("")
	parser := &testParser{buf: buf}
	fp := &Parser{WorkDir: tmpdir, Callback: parser, Silent: true}
	r, err := fp.Parse("logPosRotateNoNL", logFileName)
	if err != nil {
		t.Error(err)
	}
	if len(r) != 1 {
		t.Fatalf("initial parse result len must be 1 %v", r)
	}

	// append a line WITHOUT trailing newline
	msg2 := fmt.Sprintf("msg msg %08d", 31)
	fh, err = os.OpenFile(logFileName, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Error(err)
	}
	fh.WriteString(msg2)
	fh.Close()

	// rotate the log (rename to archive)
	archived := filepath.Join(tmpdir, "log.1")
	err = os.Rename(logFileName, archived)
	if err != nil {
		t.Error(err)
	}

	// create a new log file and write another line
	fh, err = os.Create(logFileName)
	if err != nil {
		t.Error(err)
	}
	msg3 := fmt.Sprintf("msg msg %08d\n", 32)
	fh.WriteString(msg3)
	fh.Close()

	// parse again: it should find the archived file and read msg2 (no newline)
	buf2 := bytes.NewBufferString("")
	parser2 := &testParser{buf: buf2}
	fp2 := &Parser{WorkDir: tmpdir, Callback: parser2, Silent: true}
	r2, err := fp2.Parse("logPosRotateNoNL", logFileName)
	if err != nil {
		t.Error(err)
	}
	out := parser2.Slurp().String()
	expected := msg2 + "\n" + msg3
	if out != expected {
		t.Fatalf("rotate read '%s' not match expect '%s'", out, expected)
	}
	if len(r2) != 2 {
		t.Fatalf("rotate result len must be 2 %v", r2)
	}
	if r2[0].Rows != 1 || r2[1].Rows != 1 {
		t.Fatalf("rotate rows must be 1 each %v", r2)
	}
}

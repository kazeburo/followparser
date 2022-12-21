package followparser

import "io"

type reader struct {
	Pos    int64
	reader io.Reader
}

func newReader(ir io.Reader, pos int64) (*reader, error) {
	if is, ok := ir.(io.Seeker); ok {
		_, err := is.Seek(pos, 0)
		if err != nil {
			return nil, err
		}
	}
	return &reader{
		Pos:    pos,
		reader: ir,
	}, nil
}

func (r *reader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.Pos += int64(n)
	return n, err
}

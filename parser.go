package followparser

type dummyParser struct{}

func (p *dummyParser) Parse(b []byte) error {
	return nil
}

func (p *dummyParser) Finish(duration float64) {
}

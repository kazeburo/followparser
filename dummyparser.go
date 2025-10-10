package followparser

type dummyParser struct{}

func (p *dummyParser) Parse(_ []byte) error {
	return nil
}

func (p *dummyParser) Finish(_ float64) {
}

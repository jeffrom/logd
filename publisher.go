package logd

type publisher struct {
	conns []*conn
}

func newPublisher() *publisher {
	return &publisher{}
}

func (p *publisher) addConn(c *conn) {
	p.conns = append(p.conns, c)
}

func (p *publisher) removeConn(c *conn) {
	conns := p.conns
	for i, pc := range conns {
		if pc == c {
			// swap this conn with the last element and slice it out
			conns[len(conns)-1], conns[i] = conns[i], conns[len(conns)-1]
			p.conns = conns[:len(conns)-1]
		}
	}
}

func (p *publisher) Write(b []byte) (int, error) {
	return 0, nil
}

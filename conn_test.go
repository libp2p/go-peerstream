package peerstream

import (
	"errors"
	"sync"
	"testing"

	iconn "github.com/libp2p/go-libp2p-interface-conn"
	smux "github.com/libp2p/go-stream-muxer"
)

func newFakeconn() *fakeconn {
	return &fakeconn{
		closed: make(chan struct{}),
	}
}

type fakeconn struct {
	iconn.Conn

	closeLock sync.Mutex
	closed    chan struct{}
}

func (fsc *fakeconn) IsClosed() bool {
	select {
	case <-fsc.closed:
		return true
	default:
		return false
	}
}

// AcceptStream accepts a stream opened by the other side.
func (fsc *fakeconn) AcceptStream() (smux.Stream, error) {
	<-fsc.closed
	return nil, errors.New("connection closed")
}

func (fsc *fakeconn) OpenStream() (smux.Stream, error) {
	if fsc.IsClosed() {
		return nil, errors.New("connection closed")
	}
	return &fakeSmuxStream{conn: fsc, closed: make(chan struct{})}, nil
}

func (fsc *fakeconn) Close() error {
	fsc.closeLock.Lock()
	defer fsc.closeLock.Unlock()
	if fsc.IsClosed() {
		return errors.New("already closed")
	}
	close(fsc.closed)
	return nil
}

var _ iconn.Conn = (*fakeconn)(nil)

func TestConnBasic(t *testing.T) {
	s := NewSwarm()
	nc := newFakeconn()
	c, err := s.AddConn(nc)
	if err != nil {
		t.Fatal(err)
	}
	if c.Swarm() != s {
		t.Fatalf("incorrect swarm returned from connection")
	}
	if sc, ok := c.Conn().(*fakeconn); !ok {
		t.Fatalf("expected a fakeconn, got %v", sc)
	}
}

func TestConnsWithGroup(t *testing.T) {
	s := NewSwarm()
	a := newConn(newFakeconn(), s)
	b := newConn(newFakeconn(), s)
	c := newConn(newFakeconn(), s)
	g := "foo"

	b.Conn().Close()
	c.Conn().Close()

	s.conns[a] = struct{}{}
	s.conns[b] = struct{}{}
	s.conns[c] = struct{}{}

	a.AddGroup(g)
	b.AddGroup(g)
	c.AddGroup(g)

	conns := s.ConnsWithGroup(g)
	if len(conns) != 1 {
		t.Fatal("should have only gotten one")
	}
	groups := a.Groups()
	if len(groups) != 1 {
		t.Fatal("should have only gotten one")
	}
	if groups[0] != g {
		t.Fatalf("expected group '%v', got group '%v'", g, groups[0])
	}

	b.closingLock.Lock()
	defer b.closingLock.Unlock()
	c.closingLock.Lock()
	defer c.closingLock.Unlock()
	if !b.closing {
		t.Fatal("b should at least be closing")
	}

	if !c.closing {
		t.Fatal("c should at least be closing")
	}
}

func TestConnIdx(t *testing.T) {
	s := NewSwarm()
	c, err := s.AddConn(newFakeconn())
	if err != nil {
		t.Fatal(err)
	}

	g := "foo"
	g2 := "bar"

	if len(s.ConnsWithGroup(g)) != 0 {
		t.Fatal("should have gotten none")
	}

	c.AddGroup(g)
	if !c.InGroup(g) {
		t.Fatal("should be in the appropriate group")
	}
	if len(s.ConnsWithGroup(g)) != 1 {
		t.Fatal("should have only gotten one")
	}

	c.Close()
	if !c.InGroup(g) {
		t.Fatal("should still be in the appropriate group")
	}
	if len(s.ConnsWithGroup(g)) != 0 {
		t.Fatal("should have gotten none")
	}

	c.AddGroup(g2)
	if !c.InGroup(g2) {
		t.Fatal("should now be in group 2")
	}
	if c.InGroup("bla") {
		t.Fatal("should not be in arbitrary groups")
	}
	if len(s.ConnsWithGroup(g)) != 0 {
		t.Fatal("should still have gotten none")
	}
	if len(s.ConnsWithGroup(g2)) != 0 {
		t.Fatal("should still have gotten none")
	}
	if len(s.connIdx) != 0 {
		t.Fatal("should have an empty index")
	}
	if len(s.conns) != 0 {
		t.Fatal("should not be holding any connections")
	}
}

func TestAddConnWithGroups(t *testing.T) {
	s := NewSwarm()

	g := "foo"
	g2 := "bar"
	g3 := "baz"

	c, err := s.AddConn(newFakeconn(), g, g2)
	if !c.InGroup(g) || !c.InGroup(g2) || c.InGroup(g3) {
		t.Fatal("should be in the appropriate groups")
	}
	if err != nil {
		t.Fatal(err)
	}

	if len(s.ConnsWithGroup(g)) != 1 {
		t.Fatal("should have gotten one")
	}

	if len(s.ConnsWithGroup(g2)) != 1 {
		t.Fatal("should have gotten one")
	}

	if len(s.ConnsWithGroup(g3)) != 0 {
		t.Fatal("should have gotten none")
	}

	c.Close()
	if len(s.ConnsWithGroup(g)) != 0 {
		t.Fatal("should have gotten none")
	}

	if len(s.ConnsWithGroup(g2)) != 0 {
		t.Fatal("should have gotten none")
	}

	if len(s.ConnsWithGroup(g3)) != 0 {
		t.Fatal("should still have gotten none")
	}

	if len(s.connIdx) != 0 {
		t.Fatal("should have an empty index")
	}
	if len(s.conns) != 0 {
		t.Fatal("should not be holding any connections")
	}
}

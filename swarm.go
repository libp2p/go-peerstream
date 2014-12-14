package peerstream

import (
	"atomic"
	"net"
	"net/http"
	"unsafe"
)

// fd is a (file) descriptor, unix style
type fd uint32

type Swarm struct {
	// active streams.
	streams    map[Stream]struct{}
	streamLock sync.RWMutex

	// active connections. generate new Streams
	conns    map[Conn]struct{}
	connLock sync.RWMutex

	// active listeners. generate new Listeners
	listeners    map[Listener]struct{}
	listenerLock sync.RWMutex

	// selectConn is the default SelectConn function
	selectConn SelectConn

	// streamHandler receives Streams initiated remotely
	// should be accessed with SetStreamHandler / StreamHandler
	// as this pointer may be changed at any time.
	streamHandler StreamHandler
}

// SetStreamHandler assigns the stream handler in the swarm.
// The handler assumes responsibility for closing the stream.
// This need not happen at the end of the handler, leaving the
// stream open (to be used and closed later) is fine.
// It is also fine to keep a pointer to the Stream.
// This is a threadsafe (atomic) operation
func (s *Swarm) SetStreamHandler(sh StreamHandler) {
	atomic.SwapPointer((*unsafe.Pointer)(s.streamHandler), (*unsafe.Pointer)(sh))
}

// StreamHandler returns the Swarm's current StreamHandler.
// This is a threadsafe (atomic) operation
func (s *Swarm) StreamHandler() StreamHandler {
	return StreamHandler(atomic.LoadPointer((*unsafe.Pointer)(s.streamHandler)))
}

// SetConnSelect assigns the connection selector in the swarm.
// This is a threadsafe (atomic) operation
func (s *Swarm) SetSelectConn(cs SelectConn) {
	atomic.SwapPointer((*unsafe.Pointer)(s.selectConn), (*unsafe.Pointer)(cs))
}

// ConnSelect returns the Swarm's current connection selector.
// ConnSelect is used in order to select the best of a set of
// possible connections. The default chooses one at random.
// This is a threadsafe (atomic) operation
func (s *Swarm) SelectConn() StreamHandler {
	return StreamHandler(atomic.LoadPointer((*unsafe.Pointer)(s.selectConn)))
}

// Conns returns all the connections associated with this Swarm.
func (s *Swarm) Conns() []Conn {
	conns := make([]Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	return conns
}

// Listeners returns all the listeners associated with this Swarm.
func (s *Swarm) Listeners() []Listener {
	out := make([]Listener, 0, len(s.listeners))
	for c := range s.listeners {
		out = append(out, c)
	}
	return out
}

// Streams returns all the streams associated with this Swarm.
func (s *Swarm) Streams() []Stream {
	out := make([]Stream, 0, len(s.streams))
	for c := range s.streams {
		out = append(out, c)
	}
	return out
}

// AddListener adds net.Listener to the Swarm, and immediately begins
// accepting incoming connections.
func (s *Swarm) AddListener(net.Listener) error {
	panic("nyi")
}

// AddListenerWithRateLimit adds Listener to the Swarm, and immediately
// begins accepting incoming connections. The rate of connection acceptance
// depends on the RateLimit option
// func (s *Swarm) AddListenerWithRateLimit(net.Listner, RateLimit) // TODO

// AddConn gives the Swarm ownership of net.Conn. The Swarm will open a
// SPDY session and begin listening for Streams.
// Returns the resulting Swarm-associated peerstream.Conn.
// Idempotent: if the Connection has already been added, this is a no-op.
func (s *Swarm) AddConn(net.Conn) (Conn, error) {
	panic("nyi")
}

// NewStream opens a new Stream on the best available connection,
// as selected by current swarm.SelectConn.
func (s *Swarm) NewStream() (Stream, error) {
	return s.NewStreamSelectConn(s.SelectConn())
}

func (s *Swarm) newStreamSelectConn(selConn SelectConn, conns []Conn) (Stream, error) {
	if selConn == nil {
		return nil, errors.New("nil SelectConn")
	}

	best := selConn(conns)
	if best == nil || !ConnInConns(best, conns) {
		return nil, ErrInvalidConnSelected
	}
	return s.NewStreamWithConn(best)
}

// NewStreamWithSelectConn opens a new Stream on a connection selected
// by selConn.
func (s *Swarm) NewStreamSelectConn(selConn SelectConn) (Stream, error) {
	conns := s.Conns()
	if len(conns) == nil {
		return nil, ErrNoConnections
	}
	return s.newStreamSelectConn(selConn, conns)
}

// NewStreamWithGroup opens a new Stream on an available connection in
// the given group. Uses the current swarm.SelectConn to pick between
// multiple connections.
func (s *Swarm) NewStreamWithGroup(group GroupID) (Stream, error) {
	g := s.connGrps.Get(group)
	if g == nil {
		return nil, ErrGroupNotFound
	}

	conns := grpblsToConns(g.GetAll())
	return s.newStreamSelectConn(s.SelectConn(), conns)
}

// NewStreamWithNetConn opens a new Stream on given net.Conn.
// Calls s.AddConn(netConn).
func (s *Swarm) NewStreamWithNetConn(netConn net.Conn) (Stream, error) {
	c, err := s.AddConn(netConn)
	if err != nil {
		return nil, err
	}
	return s.NewStreamWithConn(c)
}

// NewStreamWithConnection opens a new Stream on given connection.
func (s *Swarm) NewStreamWithConn(conn Conn) (Stream, error) {
	if conn == nil {
		return nil, errors.New("nil Conn")
	}
	if conn.Swarm() != s {
		return nil, errors.New("connection not associated with swarm")
	}

	s.connsLock.RLock()
	if _, found := s.conns[conn]; !found {
		s.connsLock.RUnlock()
		return nil, errors.New("connection not associated with swarm")
	}
	s.connsLock.RUnlock()

	iconn, ok := conn.(*Conn)
	if !ok {
		return nil, errors.New("invalid conn")
	}

	return s.setupStream(iconn)
}

// newStream is the internal function that creates a new stream. assumes
// all validation has happened.
func (s *Swarm) setupStream(c *conn) (Stream, error) {

	// Create a new ss.Stream
	ssStream, err := c.ssConn.CreateStream(http.Header{}, nil, false)
	if err != nil {
		return nil, err
	}

	stream := newStream(c)
	return stream, nil
}

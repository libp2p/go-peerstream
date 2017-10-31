package peerstream

import (
	"errors"
	"fmt"
	"sync"
	"time"

	iconn "github.com/libp2p/go-libp2p-interface-conn"
	smux "github.com/libp2p/go-stream-muxer"
)

// GarbageCollectTimeout governs the periodic connection closer.
var GarbageCollectTimeout = 5 * time.Second

// Swarm represents a group of streams, connections and listeners which
// are interconnected using a multiplexed transport.
// Swarms keep track of user-added handlers which
// define the actions upon the arrival of new streams and handlers.
type Swarm struct {
	// active streams.
	streams    map[*Stream]struct{}
	streamLock sync.RWMutex

	// active connections. generate new Streams
	conns    map[*Conn]struct{}
	connIdx  map[Group]map[*Conn]struct{}
	connLock sync.RWMutex

	// active listeners. generate new Listeners
	listeners    map[*Listener]struct{}
	listenerLock sync.RWMutex

	// these handlers should be accessed with their getter/setter
	// as this pointer may be changed at any time.
	handlerLock   sync.RWMutex  // protects the functions below
	connHandler   ConnHandler   // receives Conns intiated remotely
	streamHandler StreamHandler // receives Streams initiated remotely
	selectConn    SelectConn    // default SelectConn function

	// notification listeners
	notifiees    map[Notifiee]struct{}
	notifieeLock sync.Mutex

	closed chan struct{}
}

// NewSwarm creates a new swarm.
func NewSwarm() *Swarm {
	s := &Swarm{
		streams:       make(map[*Stream]struct{}),
		conns:         make(map[*Conn]struct{}),
		connIdx:       make(map[Group]map[*Conn]struct{}),
		listeners:     make(map[*Listener]struct{}),
		notifiees:     make(map[Notifiee]struct{}),
		selectConn:    SelectRandomConn,
		streamHandler: NoOpStreamHandler,
		connHandler:   NoOpConnHandler,
		closed:        make(chan struct{}),
	}
	go s.connGarbageCollect()
	return s
}

// String returns a string with various internal stats
func (s *Swarm) String() string {
	s.listenerLock.RLock()
	ls := len(s.listeners)
	s.listenerLock.RUnlock()

	s.connLock.RLock()
	cs := len(s.conns)
	s.connLock.RUnlock()

	s.streamLock.RLock()
	ss := len(s.streams)
	s.streamLock.RUnlock()

	str := "<peerstream.Swarm %d listeners %d conns %d streams>"
	return fmt.Sprintf(str, ls, cs, ss)
}

// Dump returns a string with all the internal state
func (s *Swarm) Dump() string {
	str := s.String() + "\n"

	s.listenerLock.RLock()
	for l := range s.listeners {
		str += fmt.Sprintf("\t%s %v\n", l, l.Groups())
	}
	s.listenerLock.RUnlock()

	s.connLock.RLock()
	for c := range s.conns {
		str += fmt.Sprintf("\t%s %v\n", c, c.Groups())
	}
	s.connLock.RUnlock()

	s.streamLock.RLock()
	for ss := range s.streams {
		str += fmt.Sprintf("\t%s %v\n", ss, ss.Groups())
	}
	s.streamLock.RUnlock()

	return str
}

// SetStreamHandler assigns the stream handler in the swarm.
// The handler assumes responsibility for closing the stream.
// This need not happen at the end of the handler, leaving the
// stream open (to be used and closed later) is fine.
// It is also fine to keep a pointer to the Stream.
// This is a threadsafe (atomic) operation
func (s *Swarm) SetStreamHandler(sh StreamHandler) {
	s.handlerLock.Lock()
	defer s.handlerLock.Unlock()
	s.streamHandler = sh
}

// StreamHandler returns the Swarm's current StreamHandler.
// This is a threadsafe (atomic) operation
func (s *Swarm) StreamHandler() StreamHandler {
	s.handlerLock.RLock()
	defer s.handlerLock.RUnlock()
	if s.streamHandler == nil {
		return NoOpStreamHandler
	}
	return s.streamHandler
}

// SetConnHandler assigns the conn handler in the swarm.
// Unlike the StreamHandler, the ConnHandler has less respon-
// ibility for the Connection. The Swarm is still its client.
// This handler is only a notification.
// This is a threadsafe (atomic) operation
func (s *Swarm) SetConnHandler(ch ConnHandler) {
	s.handlerLock.Lock()
	defer s.handlerLock.Unlock()
	s.connHandler = ch
}

// ConnHandler returns the Swarm's current ConnHandler.
// This is a threadsafe (atomic) operation
func (s *Swarm) ConnHandler() ConnHandler {
	s.handlerLock.RLock()
	defer s.handlerLock.RUnlock()
	if s.connHandler == nil {
		return NoOpConnHandler
	}
	return s.connHandler
}

// SetSelectConn assigns the connection selector in the swarm.
// If cs is nil, will use SelectRandomConn
// This is a threadsafe (atomic) operation
func (s *Swarm) SetSelectConn(cs SelectConn) {
	s.handlerLock.Lock()
	defer s.handlerLock.Unlock()
	s.selectConn = cs
}

// SelectConn returns the Swarm's current connection selector.
// SelectConn is used in order to select the best of a set of
// possible connections. The default chooses one at random.
// This is a threadsafe (atomic) operation
func (s *Swarm) SelectConn() SelectConn {
	s.handlerLock.RLock()
	defer s.handlerLock.RUnlock()
	if s.selectConn == nil {
		return SelectRandomConn
	}
	return s.selectConn
}

// Conns returns all the connections associated with this Swarm.
func (s *Swarm) Conns() []*Conn {
	conns := make([]*Conn, 0, len(s.conns))
	s.connLock.RLock()
	for c := range s.conns {
		if !c.conn.IsClosed() {
			conns = append(conns, c)
		}
	}
	s.connLock.RUnlock()

	return conns
}

// ConnsWithGroup returns all the connections with a given Group
func (s *Swarm) ConnsWithGroup(g Group) []*Conn {
	s.connLock.RLock()
	cs := s.connIdx[g]
	conns := make([]*Conn, 0, len(cs))
	for c := range cs {
		conns = append(conns, c)
	}
	s.connLock.RUnlock()

	for i := 0; i < len(conns); {
		c := conns[i]
		if c.conn.IsClosed() {
			c.GoClose()
			conns[i] = conns[len(conns)-1]
			conns[len(conns)-1] = nil
			conns = conns[:len(conns)-1]
		} else {
			i++
		}
	}

	return conns
}

// Listeners returns all the listeners associated with this Swarm.
func (s *Swarm) Listeners() []*Listener {
	s.listenerLock.RLock()
	out := make([]*Listener, 0, len(s.listeners))
	for c := range s.listeners {
		out = append(out, c)
	}
	s.listenerLock.RUnlock()
	return out
}

// Streams returns all the streams associated with this Swarm.
func (s *Swarm) Streams() []*Stream {
	s.streamLock.RLock()
	out := make([]*Stream, 0, len(s.streams))
	for c := range s.streams {
		out = append(out, c)
	}
	s.streamLock.RUnlock()
	return out
}

// AddListener adds iconn.Listener to the Swarm, and immediately begins
// accepting incoming connections.
func (s *Swarm) AddListener(l iconn.Listener, groups ...Group) (*Listener, error) {
	return s.addListener(l, groups)
}

// AddListenerWithRateLimit adds Listener to the Swarm, and immediately
// begins accepting incoming connections. The rate of connection acceptance
// depends on the RateLimit option
// func (s *Swarm) AddListenerWithRateLimit(net.Listner, RateLimit) // TODO

// AddConn gives the Swarm ownership of iconn.Conn
// Returns the resulting Swarm-associated peerstream.Conn.
// Idempotent: if the Connection has already been added, this is a no-op.
func (s *Swarm) AddConn(conn iconn.Conn, groups ...Group) (*Conn, error) {
	if conn == nil {
		return nil, errors.New("nil conn")
	}

	// first, check if we already have it, to avoid constructing it
	// if it is already there
	s.connLock.Lock()
	for c := range s.conns {
		if c.conn == conn {
			s.connLock.Unlock()
			return c, nil
		}
	}
	c := newConn(conn, s)
	s.conns[c] = struct{}{}
	for _, g := range groups {
		c.groups.m[g] = struct{}{}
		c.addGroup(g)
	}
	s.connLock.Unlock()

	s.ConnHandler()(c)

	// go listen for incoming streams on this connection
	go func() {
		for {
			str, err := c.conn.AcceptStream()
			if err != nil {
				break
			}
			go func() {
				stream := s.setupStream(str, c)
				s.StreamHandler()(stream) // call our handler
			}()
		}
	}()

	s.notifyAll(func(n Notifiee) {
		n.Connected(c)
	})
	return c, nil
}

// newStream is the internal function that creates a new stream. assumes
// all validation has happened.
func (s *Swarm) setupStream(smuxStream smux.Stream, c *Conn) *Stream {
	stream := newStream(smuxStream, c)

	// add it to our streams maps
	s.streamLock.Lock()
	c.streamLock.Lock()
	s.streams[stream] = struct{}{}
	c.streams[stream] = struct{}{}
	s.streamLock.Unlock()
	c.streamLock.Unlock()

	s.notifyAll(func(n Notifiee) {
		n.OpenedStream(stream)
	})
	return stream
}

// createStream is the internal function that creates a new stream. assumes
// all validation has happened.
func (s *Swarm) createStream(c *Conn) (*Stream, error) {
	smuxStream, err := c.conn.OpenStream()
	if err != nil {
		return nil, err
	}

	return s.setupStream(smuxStream, c), nil
}

// TODO: Really, we need to either not track them here or, possibly, add a
// notification system to go-stream-muxer (shudder).
// Alternatively, we could garbage collect them like we do connections but then
// we'd need a way to determine which connections are open (we'd need IsClosed)
// methods.
func (s *Swarm) removeStream(stream *Stream, reset bool) error {
	// remove from our maps
	s.streamLock.Lock()
	_, isOpen := s.streams[stream]
	if isOpen {
		stream.conn.streamLock.Lock()
		delete(s.streams, stream)
		delete(stream.conn.streams, stream)
		stream.conn.streamLock.Unlock()
	}
	s.streamLock.Unlock()

	var err error
	if reset {
		err = stream.smuxStream.Reset()
	} else {
		err = stream.smuxStream.Close()
	}
	if isOpen {
		s.notifyAll(func(n Notifiee) {
			n.ClosedStream(stream)
		})
	}
	return err
}

func (s *Swarm) removeConn(conn *Conn) {
	// remove from our maps
	s.connLock.Lock()
	defer s.connLock.Unlock()
	delete(s.conns, conn)
	for _, g := range conn.groups.Groups() {
		conn.removeGroup(g)
	}
}

// NewStream opens a new Stream on the best available connection,
// as selected by current swarm.SelectConn.
func (s *Swarm) NewStream() (*Stream, error) {
	return s.NewStreamSelectConn(s.SelectConn())
}

func (s *Swarm) newStreamSelectConn(selConn SelectConn, conns []*Conn) (*Stream, error) {
	if selConn == nil {
		return nil, errors.New("nil SelectConn")
	}

	best := selConn(conns)
	if best == nil || !ConnInConns(best, conns) {
		return nil, ErrInvalidConnSelected
	}
	return s.NewStreamWithConn(best)
}

// NewStreamSelectConn opens a new Stream on a connection selected
// by selConn.
func (s *Swarm) NewStreamSelectConn(selConn SelectConn) (*Stream, error) {
	if selConn == nil {
		return nil, errors.New("nil SelectConn")
	}

	conns := s.Conns()
	if len(conns) == 0 {
		return nil, ErrNoConnections
	}
	return s.newStreamSelectConn(selConn, conns)
}

// NewStreamWithGroup opens a new Stream on an available connection in
// the given group. Uses the current swarm.SelectConn to pick between
// multiple connections.
func (s *Swarm) NewStreamWithGroup(group Group) (*Stream, error) {
	conns := s.ConnsWithGroup(group)
	return s.newStreamSelectConn(s.SelectConn(), conns)
}

// NewStreamWithConn opens a new Stream on given Conn.
func (s *Swarm) NewStreamWithConn(conn *Conn) (*Stream, error) {
	if conn == nil {
		return nil, errors.New("nil Conn")
	}
	if conn.Swarm() != s {
		return nil, errors.New("connection not associated with swarm")
	}

	if conn.conn.IsClosed() {
		go conn.Close()
		return nil, errors.New("conn is closed")
	}

	s.connLock.RLock()
	if _, found := s.conns[conn]; !found {
		s.connLock.RUnlock()
		return nil, errors.New("connection not associated with swarm")
	}
	s.connLock.RUnlock()
	return s.createStream(conn)
}

// StreamsWithGroup returns all the streams with a given Group
func (s *Swarm) StreamsWithGroup(g Group) []*Stream {
	return StreamsWithGroup(g, s.Streams())
}

// Close shuts down the Swarm, and it's listeners.
func (s *Swarm) Close() error {
	defer close(s.closed)

	// automatically close everything new we get.
	s.SetConnHandler(func(c *Conn) { c.Close() })
	s.SetStreamHandler(func(s *Stream) { s.Reset() })

	var wgl sync.WaitGroup
	for _, l := range s.Listeners() {
		wgl.Add(1)
		go func(list *Listener) {
			list.Close()
			wgl.Done()
		}(l)
	}
	wgl.Wait()

	var wgc sync.WaitGroup
	for _, c := range s.Conns() {
		wgc.Add(1)
		go func(conn *Conn) {
			conn.Close()
			wgc.Done()
		}(c)
	}
	wgc.Wait()
	return nil
}

// connGarbageCollect periodically sweeps conns to make sure
// they're still alive. if any are closed, remvoes them.
func (s *Swarm) connGarbageCollect() {
	for {
		select {
		case <-s.closed:
			return
		case <-time.After(GarbageCollectTimeout):
		}

		for _, c := range s.Conns() {
			if c.conn.IsClosed() {
				go c.Close()
			}
		}
	}
}

// Notify signs up Notifiee to receive signals when events happen
func (s *Swarm) Notify(n Notifiee) {
	s.notifieeLock.Lock()
	s.notifiees[n] = struct{}{}
	s.notifieeLock.Unlock()
}

// StopNotify unregisters Notifiee fromr receiving signals
func (s *Swarm) StopNotify(n Notifiee) {
	s.notifieeLock.Lock()
	delete(s.notifiees, n)
	s.notifieeLock.Unlock()
}

// notifyAll runs the notification function on all Notifiees
func (s *Swarm) notifyAll(notification func(n Notifiee)) {
	s.notifieeLock.Lock()
	var wg sync.WaitGroup
	for n := range s.notifiees {
		// make sure we dont block
		// and they dont block each other.
		wg.Add(1)
		go func(n Notifiee) {
			defer wg.Done()
			notification(n)
		}(n)
	}
	wg.Wait()
	s.notifieeLock.Unlock()
}

// Notifiee is an interface for an object wishing to receive
// notifications from a Swarm. Notifiees should take care not to register other
// notifiees inside of a notification.  They should also take care to do as
// little work as possible within their notification, putting any blocking work
// out into a goroutine.
type Notifiee interface {
	Connected(*Conn)      // called when a connection opened
	Disconnected(*Conn)   // called when a connection closed
	OpenedStream(*Stream) // called when a stream opened
	ClosedStream(*Stream) // called when a stream closed
}

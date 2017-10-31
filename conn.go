package peerstream

import (
	"errors"
	"fmt"
	"sync"

	iconn "github.com/libp2p/go-libp2p-interface-conn"
)

// ConnHandler is a function which receives a Conn. It allows
// clients to set a function to receive newly accepted
// connections. It works like StreamHandler, but is usually
// less useful than usual as most services will only use
// Streams. It is safe to pass or store the *Conn elsewhere.
// Note: the ConnHandler is called sequentially, so spawn
// goroutines or pass the Conn. See EchoHandler.
type ConnHandler func(s *Conn)

// SelectConn selects a connection out of list. It allows
// delegation of decision making to clients. Clients can
// make SelectConn functons that check things connection
// qualities -- like latency andbandwidth -- or pick from
// a logical set of connections.
type SelectConn func([]*Conn) *Conn

// ErrInvalidConnSelected signals that a connection selected
// with a SelectConn function is invalid. This may be due to
// the Conn not being part of the original set given to the
// function, or the value being nil.
var ErrInvalidConnSelected = errors.New("invalid selected connection")

// ErrNoConnections signals that no connections are available
var ErrNoConnections = errors.New("no connections")

// Conn is a Swarm-associated connection.
type Conn struct {
	conn iconn.Conn

	swarm  *Swarm
	groups groupSet

	streams    map[*Stream]struct{}
	streamLock sync.RWMutex

	closed    bool
	closeLock sync.Mutex

	closing     bool
	closingLock sync.Mutex
}

func newConn(conn iconn.Conn, s *Swarm) *Conn {
	return &Conn{
		conn:    conn,
		swarm:   s,
		groups:  groupSet{m: make(map[Group]struct{})},
		streams: make(map[*Stream]struct{}),
	}
}

// String returns a string representation of the Conn.
func (c *Conn) String() string {
	c.streamLock.RLock()
	ls := len(c.streams)
	c.streamLock.RUnlock()
	f := "<peerstream.Conn %d streams %s <--> %s>"
	return fmt.Sprintf(f, ls, c.conn.LocalAddr(), c.conn.RemoteAddr())
}

// Swarm returns the Swarm associated with this Conn.
func (c *Conn) Swarm() *Swarm {
	return c.swarm
}

// Conn returns the underlying iconn.Conn used
// Warning: modifying this object is undefined.
func (c *Conn) Conn() iconn.Conn {
	return c.conn
}

// Groups returns the Groups this Conn belongs to.
func (c *Conn) Groups() []Group {
	return c.groups.Groups()
}

// InGroup returns whether this Conn belongs to a Group.
func (c *Conn) InGroup(g Group) bool {
	return c.groups.Has(g)
}

// AddGroup assigns given Group to Conn.
func (c *Conn) AddGroup(g Group) {
	c.swarm.connLock.Lock()
	defer c.swarm.connLock.Unlock()

	c.groups.Add(g)

	if _, ok := c.swarm.conns[c]; !ok {
		// Not being tracked.
		// DO NOT REMOVE THIS CHECK.
		// DO NOT USE MULTIPLE LOCKS.
		// YOU WILL LEAK CONNECTIONS!
		return
	}

	c.addGroup(g)
}

// NOTE: must be called under the connIdxLock lock.
func (c *Conn) removeGroup(g Group) {
	m, ok := c.swarm.connIdx[g]
	if !ok {
		return
	}
	delete(m, c)
	if len(m) == 0 {
		delete(c.swarm.connIdx, g)
	}
}

// NOTE: must be called under the connIdxLock lock.
func (c *Conn) addGroup(g Group) {
	m, ok := c.swarm.connIdx[g]
	if !ok {
		m = make(map[*Conn]struct{}, 1)
		c.swarm.connIdx[g] = m
	}
	m[c] = struct{}{}
}

// NewStream returns a stream associated with this Conn.
func (c *Conn) NewStream() (*Stream, error) {
	return c.swarm.NewStreamWithConn(c)
}

// Streams returns the slice of all streams associated to this Conn.
func (c *Conn) Streams() []*Stream {
	c.streamLock.RLock()
	defer c.streamLock.RUnlock()

	streams := make([]*Stream, 0, len(c.streams))
	for s := range c.streams {
		streams = append(streams, s)
	}
	return streams
}

// GoClose spawns off a goroutine to close the connection iff the connection is
// not already being closed and returns immediately
func (c *Conn) GoClose() {
	c.closingLock.Lock()
	defer c.closingLock.Unlock()
	if c.closing {
		return
	}
	c.closing = true

	go c.Close()
}

// Close closes this connection
func (c *Conn) Close() error {
	c.closeLock.Lock()
	defer c.closeLock.Unlock()
	if c.closed == true {
		return nil
	}

	c.closingLock.Lock()
	c.closing = true
	c.closingLock.Unlock()

	c.closed = true

	// reset streams
	streams := c.Streams()
	for _, s := range streams {
		s.Reset()
	}

	// close underlying connection
	c.swarm.removeConn(c)
	err := c.conn.Close()
	c.swarm.notifyAll(func(n Notifiee) {
		n.Disconnected(c)
	})
	return err
}

// ConnInConns returns true if a connection belongs to the
// conns slice.
func ConnInConns(c1 *Conn, conns []*Conn) bool {
	for _, c2 := range conns {
		if c2 == c1 {
			return true
		}
	}
	return false
}

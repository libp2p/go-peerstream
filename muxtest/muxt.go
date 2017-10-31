package muxtest

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"fmt"
	"io"
	mrand "math/rand"
	"os"
	"reflect"
	"runtime"
	"sync"
	"testing"

	peer "github.com/libp2p/go-libp2p-peer"
	ps "github.com/libp2p/go-peerstream"

	conn "github.com/libp2p/go-libp2p-conn"
	iconn "github.com/libp2p/go-libp2p-interface-conn"
	tpt "github.com/libp2p/go-libp2p-transport"
	smux "github.com/libp2p/go-stream-muxer"
	testutil "github.com/libp2p/go-testutil"
	ma "github.com/multiformats/go-multiaddr"
)

var randomness []byte
var nextPort = 20000
var verbose = false

func init() {
	// read 1MB of randomness
	randomness = make([]byte, 1<<20)
	if _, err := crand.Read(randomness); err != nil {
		panic(err)
	}
}

type listenerOpts struct {
	transportCb func() tpt.Transport
	localaddr   ma.Multiaddr
}

func randBuf(size int) []byte {
	n := len(randomness) - size
	if size < 1 {
		panic(fmt.Errorf("requested too large buffer (%d). max is %d", size, len(randomness)))
	}

	start := mrand.Intn(n)
	return randomness[start : start+size]
}

func checkErr(t *testing.T, err error) {
	if err != nil {
		panic(err)
		t.Fatal(err)
	}
}

func log(s string, v ...interface{}) {
	if verbose {
		fmt.Fprintf(os.Stderr, "> "+s+"\n", v...)
	}
}

type echoSetup struct {
	swarm *ps.Swarm
	conns []*ps.Conn
}

func createConnListener(t *testing.T, lo listenerOpts, streamMuxer smux.Transport) (iconn.Listener, peer.ID) {
	identity := testutil.RandIdentityOrFatal(t)
	l, err := lo.transportCb().Listen(lo.localaddr)
	checkErr(t, err)
	cl, err := conn.WrapTransportListener(context.Background(), l, identity.ID(), streamMuxer, identity.PrivateKey())
	checkErr(t, err)
	return cl, identity.ID()
}

func singleConn(t *testing.T, lo listenerOpts, streamMuxer smux.Transport) echoSetup {
	swarm := ps.NewSwarm()
	swarm.SetStreamHandler(func(s *ps.Stream) {
		defer s.Close()
		log("accepted stream")
		io.Copy(s, s) // echo everything
		log("closing stream")
	})

	log("listening at %s", "localhost:0")
	cl, listenerID := createConnListener(t, lo, streamMuxer)
	_, err := swarm.AddListener(cl)
	checkErr(t, err)

	log("dialing to %s", cl.Multiaddr())
	dialerIdentity := testutil.RandIdentityOrFatal(t)
	cd := conn.NewDialer(dialerIdentity.ID(), dialerIdentity.PrivateKey(), nil, streamMuxer)
	dialer, err := lo.transportCb().Dialer(cl.Multiaddr())
	checkErr(t, err)
	cd.AddDialer(dialer)
	nc1, err := cd.Dial(context.Background(), cl.Multiaddr(), listenerID)
	checkErr(t, err)

	c1, err := swarm.AddConn(nc1)
	checkErr(t, err)

	return echoSetup{
		swarm: swarm,
		conns: []*ps.Conn{c1},
	}
}

func makeSwarm(t *testing.T, lo listenerOpts, streamMuxer smux.Transport, nListeners int) *ps.Swarm {
	swarm := ps.NewSwarm()
	swarm.SetStreamHandler(func(s *ps.Stream) {
		defer s.Close()
		log("accepted stream")
		io.Copy(s, s) // echo everything
		log("closing stream")
	})

	for i := 0; i < nListeners; i++ {
		log("%p listening at %s", swarm, "localhost:0")
		cl, _ := createConnListener(t, lo, streamMuxer)
		_, err := swarm.AddListener(cl)
		checkErr(t, err)
	}

	return swarm
}

func makeSwarms(t *testing.T, lo listenerOpts, streamMuxer smux.Transport, nSwarms, nListeners int) []*ps.Swarm {
	swarms := make([]*ps.Swarm, nSwarms)
	for i := 0; i < nSwarms; i++ {
		swarms[i] = makeSwarm(t, lo, streamMuxer, nListeners)
	}
	return swarms
}

func SubtestConstructSwarm(t *testing.T, lo listenerOpts, streamMuxer smux.Transport) {
	ps.NewSwarm()
}

func SubtestSimpleWrite(t *testing.T, lo listenerOpts, streamMuxer smux.Transport) {
	swarm := ps.NewSwarm()
	defer swarm.Close()

	swarm.SetStreamHandler(func(s *ps.Stream) {
		defer s.Close()
		log("accepted stream")
		io.Copy(s, s)
		log("closing stream")
	})

	log("listening at %s", "localhost:0")
	cl, listenerID := createConnListener(t, lo, streamMuxer)
	_, err := swarm.AddListener(cl)
	checkErr(t, err)

	log("dialing to %s", cl.Addr().String())
	dialerIdentity := testutil.RandIdentityOrFatal(t)
	cd := conn.NewDialer(dialerIdentity.ID(), dialerIdentity.PrivateKey(), nil, streamMuxer)
	dialer, err := lo.transportCb().Dialer(cl.Multiaddr())
	checkErr(t, err)
	cd.AddDialer(dialer)
	nc1, err := cd.Dial(context.Background(), cl.Multiaddr(), listenerID)
	checkErr(t, err)
	c1, err := swarm.AddConn(nc1)
	checkErr(t, err)
	defer c1.Close()

	log("creating stream")
	s1, err := c1.NewStream()
	checkErr(t, err)

	buf1 := randBuf(4096)
	log("writing %d bytes to stream", len(buf1))
	_, err = s1.Write(buf1)
	checkErr(t, err)

	buf2 := make([]byte, len(buf1))
	log("reading %d bytes from stream (echoed)", len(buf2))
	_, err = io.ReadFull(s1, buf2)
	checkErr(t, err)
	if string(buf2) != string(buf1) {
		t.Errorf("buf1 and buf2 not equal: %#v != %#v", buf1, buf2)
	}
}

func SubtestSimpleWrite100msgs(t *testing.T, lo listenerOpts, streamMuxer smux.Transport) {
	msgs := 100
	msgsize := 1 << 19
	es := singleConn(t, lo, streamMuxer)
	defer es.swarm.Close()

	log("creating stream")
	stream, err := es.conns[0].NewStream()
	checkErr(t, err)

	bufs := make(chan []byte, msgs)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < msgs; i++ {
			buf := randBuf(msgsize)
			bufs <- buf
			log("writing %d bytes (message %d/%d #%x)", len(buf), i, msgs, buf[:3])
			if _, err := stream.Write(buf); err != nil {
				t.Error(fmt.Errorf("stream.Write(buf): %s", err))
				continue
			}
		}
		close(bufs)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		buf2 := make([]byte, msgsize)
		i := 0
		for buf1 := range bufs {
			log("reading %d bytes (message %d/%d #%x)", len(buf1), i, msgs, buf1[:3])
			i++

			if _, err := io.ReadFull(stream, buf2); err != nil {
				t.Error(fmt.Errorf("readFull(stream, buf2): %s", err))
				continue
			}
			if !bytes.Equal(buf1, buf2) {
				t.Error(fmt.Errorf("buffers not equal (%x != %x)", buf1[:3], buf2[:3]))
			}
		}
	}()
	wg.Wait()
}

func SubtestStressNSwarmNConnNStreamNMsg(t *testing.T, lo listenerOpts, streamMuxer smux.Transport, nSwarm, nConn, nStream, nMsg int) {
	msgsize := 1 << 11

	rateLimitN := 5000
	rateLimitChan := make(chan struct{}, rateLimitN) // max of 5k funcs.
	for i := 0; i < rateLimitN; i++ {
		rateLimitChan <- struct{}{}
	}

	rateLimit := func(f func()) {
		<-rateLimitChan
		f()
		rateLimitChan <- struct{}{}
	}

	writeStream := func(s *ps.Stream, bufs chan<- []byte) {
		log("writeStream %p, %d nMsg", s, nMsg)

		for i := 0; i < nMsg; i++ {
			buf := randBuf(msgsize)
			bufs <- buf
			// log("%p writing %d bytes (message %d/%d #%x)", s, len(buf), i, nMsg, buf[:3])
			if _, err := s.Write(buf); err != nil {
				t.Error(fmt.Errorf("s.Write(buf): %s", err))
				continue
			}
		}
	}

	readStream := func(s *ps.Stream, bufs <-chan []byte) {
		log("readStream %p, %d nMsg", s, nMsg)

		buf2 := make([]byte, msgsize)
		i := 0
		for buf1 := range bufs {
			i++
			// log("%p reading %d bytes (message %d/%d #%x)", s, len(buf1), i-1, nMsg, buf1[:3])

			if _, err := io.ReadFull(s, buf2); err != nil {
				// log("%p failed to read %d bytes (message %d/%d #%x)", s, len(buf1), i-1, nMsg, buf1[:3])
				t.Error(fmt.Errorf("io.ReadFull(s, buf2): %s", err))
				continue
			}
			if !bytes.Equal(buf1, buf2) {
				t.Error(fmt.Errorf("buffers not equal (%x != %x)", buf1[:3], buf2[:3]))
			}
		}
	}

	openStreamAndRW := func(c *ps.Conn) {
		log("openStreamAndRW %p, %d nMsg", c, nMsg)

		s, err := c.NewStream()
		if err != nil {
			t.Error(fmt.Errorf("Failed to create NewStream: %s", err))
			return
		}

		bufs := make(chan []byte, nMsg)
		go func() {
			writeStream(s, bufs)
			close(bufs)
		}()

		readStream(s, bufs)
		s.Close()
	}

	openConnAndRW := func(a, b *ps.Swarm) {
		log("openConnAndRW %p -> %p, %d nStream", a, b, nConn)

		ls := b.Listeners()
		l := ls[mrand.Intn(len(ls))]

		dialerIdentity := testutil.RandIdentityOrFatal(t)
		cd := conn.NewDialer(dialerIdentity.ID(), dialerIdentity.PrivateKey(), nil, streamMuxer)
		dialer, err := lo.transportCb().Dialer(l.Multiaddr())
		checkErr(t, err)
		cd.AddDialer(dialer)
		nc, err := cd.Dial(context.Background(), l.Multiaddr(), peer.ID(0))
		checkErr(t, err)

		c, err := a.AddConn(nc)
		if err != nil {
			t.Fatal(fmt.Errorf("a.AddConn(%s <--> %s): %s", nc.LocalAddr(), nc.RemoteAddr(), err))
			return
		}

		var wg sync.WaitGroup
		for i := 0; i < nStream; i++ {
			wg.Add(1)
			go rateLimit(func() {
				defer wg.Done()
				openStreamAndRW(c)
			})
		}
		wg.Wait()
		log("Closing connection")
		c.Close()
	}

	openConnsAndRW := func(a, b *ps.Swarm) {
		log("openConnsAndRW %p -> %p, %d conns", a, b, nConn)

		var wg sync.WaitGroup
		for i := 0; i < nConn; i++ {
			wg.Add(1)
			go rateLimit(func() {
				defer wg.Done()
				openConnAndRW(a, b)
			})
		}
		wg.Wait()
	}

	connectSwarmsAndRW := func(swarms []*ps.Swarm) {
		log("connectSwarmsAndRW %d swarms", len(swarms))

		var wg sync.WaitGroup
		for _, a := range swarms {
			for _, b := range swarms {
				wg.Add(1)
				a := a // race
				b := b // race
				go rateLimit(func() {
					defer wg.Done()
					openConnsAndRW(a, b)
				})
			}
		}
		wg.Wait()
	}

	swarms := makeSwarms(t, lo, streamMuxer, nSwarm, 3) // 3 listeners per swarm.
	connectSwarmsAndRW(swarms)
	for _, s := range swarms {
		s.Close()
	}

}

func SubtestStress1Swarm1Conn1Stream1Msg(t *testing.T, lo listenerOpts, streamMuxer smux.Transport) {
	SubtestStressNSwarmNConnNStreamNMsg(t, lo, streamMuxer, 1, 1, 1, 1)
}

func SubtestStress1Swarm1Conn1Stream100Msg(t *testing.T, lo listenerOpts, streamMuxer smux.Transport) {
	SubtestStressNSwarmNConnNStreamNMsg(t, lo, streamMuxer, 1, 1, 1, 100)
}

func SubtestStress1Swarm1Conn100Stream100Msg(t *testing.T, lo listenerOpts, streamMuxer smux.Transport) {
	SubtestStressNSwarmNConnNStreamNMsg(t, lo, streamMuxer, 1, 1, 300, 100)
}

func SubtestStress1Swarm10Conn50Stream50Msg(t *testing.T, lo listenerOpts, streamMuxer smux.Transport) {
	SubtestStressNSwarmNConnNStreamNMsg(t, lo, streamMuxer, 1, 10, 50, 50)
}

func SubtestStress5Swarm2Conn20Stream20Msg(t *testing.T, lo listenerOpts, streamMuxer smux.Transport) {
	SubtestStressNSwarmNConnNStreamNMsg(t, lo, streamMuxer, 5, 2, 20, 20)
}

func SubtestStress10Swarm2Conn100Stream100Msg(t *testing.T, lo listenerOpts, streamMuxer smux.Transport) {
	SubtestStressNSwarmNConnNStreamNMsg(t, lo, streamMuxer, 10, 2, 100, 100)
}

func SubtestAll(t *testing.T, lo listenerOpts, streamMuxer smux.Transport) {
	tests := []TransportTest{
		SubtestConstructSwarm,
		SubtestSimpleWrite,
		SubtestSimpleWrite100msgs,
		SubtestStress1Swarm1Conn1Stream1Msg,
		SubtestStress1Swarm1Conn1Stream100Msg,
		SubtestStress1Swarm1Conn100Stream100Msg,
		SubtestStress1Swarm10Conn50Stream50Msg,
		SubtestStress5Swarm2Conn20Stream20Msg,
		// SubtestStress10Swarm2Conn100Stream100Msg, <-- this hoses the osx network stack...
	}

	for _, f := range tests {
		if testing.Verbose() {
			fmt.Fprintf(os.Stderr, "==== RUN %s\n", GetFunctionName(f))
		}
		f(t, lo, streamMuxer)
	}
}

type TransportTest func(t *testing.T, lo listenerOpts, streamMuxer smux.Transport)

func TestNoOp(t *testing.T) {}

func GetFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

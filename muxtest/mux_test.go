package muxtest

import (
	"testing"

	tpt "github.com/libp2p/go-libp2p-transport"
	tcpc "github.com/libp2p/go-tcp-transport"
	quict "github.com/marten-seemann/libp2p-quic-transport"
	ma "github.com/multiformats/go-multiaddr"
	multistream "github.com/whyrusleeping/go-smux-multistream"
	spdy "github.com/whyrusleeping/go-smux-spdystream"
	yamux "github.com/whyrusleeping/go-smux-yamux"
)

func TestYamuxTransport(t *testing.T) {
	l := listenerOpts{
		transportCb: func() tpt.Transport { return tcpc.NewTCPTransport() },
		localaddr:   ma.StringCast("/ip4/127.0.0.1/tcp/0"),
	}
	SubtestAll(t, l, yamux.DefaultTransport)
}

func TestSpdyStreamTransport(t *testing.T) {
	l := listenerOpts{
		transportCb: func() tpt.Transport { return tcpc.NewTCPTransport() },
		localaddr:   ma.StringCast("/ip4/127.0.0.1/tcp/0"),
	}
	SubtestAll(t, l, spdy.Transport)
}

func TestQuicTransport(t *testing.T) {
	l := listenerOpts{
		transportCb: func() tpt.Transport { return quict.NewQuicTransport() },
		localaddr:   ma.StringCast("/ip4/127.0.0.1/udp/0/quic"),
	}
	SubtestAll(t, l, nil)
}

/*
func TestMultiplexTransport(t *testing.T) {
	SubtestAll(t, multiplex.DefaultTransport)
}

func TestMuxadoTransport(t *testing.T) {
	SubtestAll(t, muxado.Transport)
}
*/

func TestMultistreamTransport(t *testing.T) {
	l := listenerOpts{
		transportCb: func() tpt.Transport { return tcpc.NewTCPTransport() },
		localaddr:   ma.StringCast("/ip4/127.0.0.1/tcp/0"),
	}
	tpt := multistream.NewBlankTransport()
	tpt.AddTransport("/yamux", yamux.DefaultTransport)
	tpt.AddTransport("/spdy", spdy.Transport)
	SubtestAll(t, l, tpt)
}

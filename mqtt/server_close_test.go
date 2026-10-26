// SPDX-License-Identifier: MIT

package mqtt

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wind-c/comqtt/v2/mqtt/listeners"
	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

// connectV311 is an MQTT 3.1.1 CONNECT with a clean session, keepalive 60 s
// and client id "c1".
var connectV311 = []byte{0x10, 0x0e, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x3c, 0x00, 0x02, 'c', '1'}

// openConnections is the number of connections EstablishConnection is serving.
func (s *Server) openConnections() int {
	s.conns.Lock()
	defer s.conns.Unlock()
	return len(s.conns.open)
}

func servedServer(t *testing.T) (*Server, string) {
	t.Helper()
	s := newServer()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, s.AddListener(listeners.NewNet("t", ln)))
	require.NoError(t, s.Serve())
	return s, ln.Addr().String()
}

func closeWithin(t *testing.T, s *Server, d time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		_ = s.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("Close did not return within %s", d)
	}
}

// TestServerCloseEndsConnectionWithoutConnect: a connection that was accepted
// but never sent CONNECT is closed by Close, and a CONNECT it sends afterwards
// is not answered.
func TestServerCloseEndsConnectionWithoutConnect(t *testing.T) {
	s, addr := servedServer(t)
	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()
	require.Eventually(t, func() bool { return s.openConnections() == 1 }, time.Second, time.Millisecond)

	closeWithin(t, s, 2*time.Second)
	require.Equal(t, 0, s.openConnections())
	_, _ = c.Write(connectV311)
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	n, err := io.ReadFull(c, make([]byte, 4))
	require.Error(t, err)
	require.Zero(t, n, "CONNACK after Close")
	require.Zero(t, s.Clients.Len())
}

// TestServerCloseWaitsForConnectedClients: Close returns after connected
// clients have been disconnected and their connections have finished.
func TestServerCloseWaitsForConnectedClients(t *testing.T) {
	s, addr := servedServer(t)
	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()
	_, err = c.Write(connectV311)
	require.NoError(t, err)
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	connack := make([]byte, 4)
	_, err = io.ReadFull(c, connack)
	require.NoError(t, err)
	require.Equal(t, []byte{0x20, 0x02, 0x00, 0x00}, connack)

	closeWithin(t, s, 2*time.Second)
	require.Equal(t, 0, s.openConnections())
}

// TestServerEstablishConnectionAfterClose: a connection handed over after
// Close is closed at once.
func TestServerEstablishConnectionAfterClose(t *testing.T) {
	s, _ := servedServer(t)
	closeWithin(t, s, 2*time.Second)
	server, client := net.Pipe()
	defer client.Close()
	result := make(chan error, 1)
	go func() { result <- s.EstablishConnection("t", server) }()
	select {
	case err := <-result:
		require.ErrorIs(t, err, packets.ErrServerShuttingDown)
	case <-time.After(time.Second):
		t.Fatal("EstablishConnection served a connection after Close")
	}
	_, err := client.Write([]byte{0})
	require.Error(t, err)
}

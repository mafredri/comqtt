// SPDX-License-Identifier: MIT

package mqtt

import (
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wind-c/comqtt/v2/mqtt/listeners"
)

func connectTimeoutServer(t *testing.T, timeout time.Duration) string {
	t.Helper()
	s := newServer()
	s.Options.ConnectTimeout = timeout
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, s.AddListener(listeners.NewNet("t", ln)))
	require.NoError(t, s.Serve())
	t.Cleanup(func() { _ = s.Close() })
	return ln.Addr().String()
}

func TestServerConnectTimeoutClosesSilentConnection(t *testing.T) {
	c, err := net.Dial("tcp", connectTimeoutServer(t, 100*time.Millisecond))
	require.NoError(t, err)
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = c.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "the server should close a connection without CONNECT")
}

func TestServerConnectTimeoutKeepsConnectedClient(t *testing.T) {
	c, err := net.Dial("tcp", connectTimeoutServer(t, 100*time.Millisecond))
	require.NoError(t, err)
	defer c.Close()
	// CONNECT, MQTT 3.1.1, clean session, keepalive 60 s, client id "c1".
	_, err = c.Write([]byte{0x10, 0x0e, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x3c, 0x00, 0x02, 'c', '1'})
	require.NoError(t, err)
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	connack := make([]byte, 4)
	_, err = io.ReadFull(c, connack)
	require.NoError(t, err)
	require.Equal(t, []byte{0x20, 0x02, 0x00, 0x00}, connack)

	// Past the connect timeout, the keepalive deadline applies.
	time.Sleep(300 * time.Millisecond)
	_, err = c.Write([]byte{0xc0, 0x00}) // PINGREQ
	require.NoError(t, err)
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	pingresp := make([]byte, 2)
	_, err = io.ReadFull(c, pingresp)
	require.NoError(t, err)
	require.Equal(t, []byte{0xd0, 0x00}, pingresp)
}

func TestServerWithoutConnectTimeoutWaits(t *testing.T) {
	c, err := net.Dial("tcp", connectTimeoutServer(t, 0))
	require.NoError(t, err)
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	_, err = c.Read(make([]byte, 1))
	require.True(t, errors.Is(err, os.ErrDeadlineExceeded), "the connection should stay open: %v", err)
}

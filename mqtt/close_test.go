// SPDX-License-Identifier: MIT

package mqtt

import (
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wind-c/comqtt/v2/mqtt/listeners"
)

// connectPacket is an MQTT 3.1.1 CONNECT with a clean session, keepalive 60 s
// and a three-byte client id appended by the caller.
var connectPacket = []byte{0x10, 0x0f, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x3c, 0x00, 0x03}

// TestServerCloseDuringConnectChurn closes the server while clients connect
// and disconnect. Close used to deadlock in Clients.GetByListener, which took
// the clients read lock a second time (through Len) while a disconnecting
// client's CompareAndDelete waited for the write lock.
func TestServerCloseDuringConnectChurn(t *testing.T) {
	iterations := 100
	if testing.Short() {
		iterations = 10
	}
	for i := range iterations {
		s := newServer()
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		require.NoError(t, s.AddListener(listeners.NewNet("t", ln)))
		require.NoError(t, s.Serve())

		var stop atomic.Bool
		var connected atomic.Int64
		var wg sync.WaitGroup
		for g := range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for !stop.Load() {
					c, err := net.Dial("tcp", ln.Addr().String())
					if err != nil {
						return
					}
					_, _ = c.Write(append(append([]byte{}, connectPacket...), fmt.Sprintf("c%02d", g)...))
					_ = c.SetReadDeadline(time.Now().Add(time.Second))
					_, _ = io.ReadFull(c, make([]byte, 4))
					connected.Add(1)
					_ = c.Close()
				}
			}()
		}
		for connected.Load() < 50 {
			time.Sleep(time.Millisecond)
		}

		done := make(chan struct{})
		go func() {
			_ = s.Close()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			buf := make([]byte, 1<<20)
			t.Fatalf("iteration %d: Close did not return within 10 s\n%s", i, buf[:runtime.Stack(buf, true)])
		}
		stop.Store(true)
		wg.Wait()
	}
}

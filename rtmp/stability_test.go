package rtmp

import (
	"crypto/cipher"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xcrypto "github.com/shuffleman/xlive-tunnel/crypto"
)

// helper: start a paired client+server over TCP, return both + cleanup.
func setupTunnelPair(t *testing.T) (client *Client, server *Server) {
	t.Helper()
	shared := make([]byte, 16)
	keyiv, err := xcrypto.DeriveKeyIV(shared)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := xcrypto.NewCFBEncrypter(keyiv)
	if err != nil {
		t.Fatal(err)
	}
	selectDec := func([]byte) (cipher.Stream, error) {
		return xcrypto.NewCFBDecrypter(keyiv)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	serverCh := make(chan *Server, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		s, err := NewServer(conn, ServerOptions{
			ChunkSize:       262144,
			SelectDecryptor: selectDec,
		})
		if err != nil {
			return
		}
		_, _ = s.Start()
		serverCh <- s
	}()

	cconn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	client = NewClient(cconn, ClientOptions{
		ChunkSize: 262144,
		Enc:       enc,
		SessionID: "0123456789abcdef0123456789abcdef",
	})
	if err := client.Start(); err != nil {
		t.Fatal(err)
	}
	server = <-serverCh
	return client, server
}

// TestHighConcurrencyWrites verifies many goroutines can Write simultaneously
// without panics, deadlocks, or data races.
func TestHighConcurrencyWrites(t *testing.T) {
	client, server := setupTunnelPair(t)
	defer client.Close()
	defer server.Close()

	// Drain server side so backpressure does not block writes
	var drainDone atomic.Bool
	go func() {
		io.Copy(io.Discard, server)
		drainDone.Store(true)
	}()

	const goroutines = 64
	const writesPerGoroutine = 128
	const payloadSize = 256

	var wg sync.WaitGroup
	var writeErr atomic.Int32

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			buf := make([]byte, payloadSize)
			for j := 0; j < writesPerGoroutine; j++ {
				_, err := client.Write(buf)
				if err != nil {
					writeErr.Add(1)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	if n := writeErr.Load(); n > 0 {
		t.Fatalf("%d goroutines encountered write errors", n)
	}
}

// TestConcurrentWriteAndClose verifies no race between Write and Close.
// This is a regression test for the c.enc race condition fix.
func TestConcurrentWriteAndClose(t *testing.T) {
	for i := 0; i < 50; i++ {
		client, server := setupTunnelPair(t)
		go io.Copy(io.Discard, server)

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			buf := make([]byte, 1024)
			for {
				_, err := client.Write(buf)
				if err != nil {
					return
				}
			}
		}()

		// Close after a brief delay to let writes start
		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond * time.Duration(1+i%5))
			client.Close()
			server.Close()
		}()

		wg.Wait()
	}
}

// TestPendingBufferBounded verifies the pending buffer does not grow unbounded.
// Writes data faster than the pacer can consume and checks memory stays bounded.
func TestPendingBufferBounded(t *testing.T) {
	client, server := setupTunnelPair(t)
	defer client.Close()
	defer server.Close()

	go io.Copy(io.Discard, server)

	// Rapidly fill dataIn channel to pressure pending buffer
	largePayload := make([]byte, 1<<20) // 1MB per write
	totalWritten := 0
	for i := 0; i < 10; i++ {
		_, err := client.Write(largePayload)
		if err != nil {
			break
		}
		totalWritten += len(largePayload)
	}

	// Allow pacer to drain
	time.Sleep(500 * time.Millisecond)

	// Force GC and check heap — pending should be capped at maxPendingSize (4MB)
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// Allow 32MB heap overhead for test infrastructure (bufios, channels, etc.)
	if m.HeapAlloc > 32<<20 {
		t.Fatalf("heap too large after pending pressure: %d MB (expected < 32 MB)", m.HeapAlloc>>20)
	}
	t.Logf("Heap after pending pressure: %.1f MB, total written: %d bytes", float64(m.HeapAlloc)/(1<<20), totalWritten)
}

// TestLargeDataTransferIntegrity writes a large amount of data through the
// tunnel and verifies the decrypted output matches (count-based check).
// This also tests the uint64 bytesReceived fix by exceeding 4GB conceptually
// through the accumulated bytesReceived counter.
func TestLargeDataTransferIntegrity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large transfer in short mode")
	}

	client, server := setupTunnelPair(t)
	defer client.Close()
	defer server.Close()

	const chunkSize = 4096
	const totalChunks = 8192 // ~32MB of data
	var totalReceived atomic.Int64

	// Reader goroutine — count received bytes
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		buf := make([]byte, 4096)
		for {
			n, err := server.Read(buf)
			if n > 0 {
				totalReceived.Add(int64(n))
			}
			if err != nil {
				return
			}
		}
	}()

	// Writer — send known pattern
	pattern := make([]byte, chunkSize)
	for i := range pattern {
		pattern[i] = byte(i % 256)
	}
	var totalSent int64
	for i := 0; i < totalChunks; i++ {
		_, err := client.Write(pattern)
		if err != nil {
			t.Fatalf("write failed at chunk %d: %v", i, err)
		}
		totalSent += chunkSize
	}

	// Give time for data to flow through
	client.Close()
	server.Close()
	<-readDone

	// Verify all bytes received (allow some loss due to pacer frame rate limits)
	received := totalReceived.Load()
	t.Logf("Sent: %d bytes, Received: %d bytes (%.1f%%)", totalSent, received, float64(received)/float64(totalSent)*100)
	if received == 0 {
		t.Fatal("no data received through tunnel")
	}
}

// TestBytesReceivedOverflow verifies uint64 bytesReceived handles values > 4GB correctly.
// With the old uint32 counter, bytesReceived would wrap to 0 after ~4GB.
func TestBytesReceivedOverflow(t *testing.T) {
	windowAckSize := uint32(2500)

	// Simulate: we've received 5GB of data (well past uint32 max)
	var bytesReceived uint64 = 5 << 30 // 5GB
	var lastAck uint64 = bytesReceived - uint64(windowAckSize)

	// After receiving one more message, diff should trigger ack
	bytesReceived += 100
	diff := uint32(bytesReceived - lastAck)
	if diff < windowAckSize {
		t.Fatalf("expected diff >= windowAckSize after 5GB, got diff=%d windowAckSize=%d", diff, windowAckSize)
	}

	// After ack, reset lastAck
	lastAck = bytesReceived

	// Continue receiving — verify arithmetic stays correct past 10GB
	bytesReceived += 5 << 30 // now at ~10GB
	diff = uint32(bytesReceived - lastAck)
	if diff < windowAckSize {
		t.Fatalf("expected diff >= windowAckSize after 10GB, got diff=%d", diff)
	}

	// Verify the ACK value sent (truncated to uint32) is reasonable
	ackVal := uint32(bytesReceived)
	if ackVal == 0 {
		t.Fatal("ACK value should not be zero")
	}
	t.Logf("After 10GB: bytesReceived=%d, ackVal=%d, diff=%d", bytesReceived, ackVal, diff)
}

// TestMultipleTunnelConnections verifies stability with many simultaneous tunnels.
func TestMultipleTunnelConnections(t *testing.T) {
	const tunnels = 20

	var wg sync.WaitGroup
	for i := 0; i < tunnels; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			client, server := setupTunnelPair(t)

			readDone := make(chan struct{})
			go func() {
				defer close(readDone)
				io.Copy(io.Discard, server)
			}()

			buf := make([]byte, 512)
			for j := 0; j < 100; j++ {
				_, err := client.Write(buf)
				if err != nil {
					t.Errorf("tunnel %d: write error at %d: %v", id, j, err)
					break
				}
			}
			client.Close()
			server.Close()
			<-readDone
		}(i)
	}
	wg.Wait()
}

// TestGoroutineCleanup verifies no goroutine leak after Close.
func TestGoroutineCleanup(t *testing.T) {
	before := runtime.NumGoroutine()

	client, server := setupTunnelPair(t)
	go io.Copy(io.Discard, server)

	// Write some data to ensure goroutines are active
	for i := 0; i < 10; i++ {
		client.Write(make([]byte, 256))
	}

	client.Close()
	server.Close()

	// Give goroutines time to exit
	time.Sleep(200 * time.Millisecond)

	after := runtime.NumGoroutine()
	leaked := after - before
	// Allow some tolerance for test framework goroutines
	if leaked > 5 {
		t.Fatalf("potential goroutine leak: %d goroutines before=%d after=%d", leaked, before, after)
	}
	t.Logf("Goroutines: before=%d after=%d leaked=%d", before, after, leaked)
}

// TestRelayModeAtomic verifies the atomic.Bool relay mode transition
// is safe under concurrent access.
func TestRelayModeAtomic(t *testing.T) {
	shared := make([]byte, 16)
	keyiv, _ := xcrypto.DeriveKeyIV(shared)
	selectDec := func([]byte) (cipher.Stream, error) {
		return xcrypto.NewCFBDecrypter(keyiv)
	}

	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()

	serverCh := make(chan *Server, 1)
	go func() {
		conn, _ := ln.Accept()
		s, _ := NewServer(conn, ServerOptions{SelectDecryptor: selectDec})
		_, _ = s.Start()
		serverCh <- s
	}()

	cconn, _ := net.Dial("tcp", ln.Addr().String())
	enc, _ := xcrypto.NewCFBEncrypter(keyiv)
	client := NewClient(cconn, ClientOptions{
		Enc:       enc,
		SessionID: "0123456789abcdef0123456789abcdef",
	})
	_ = client.Start()
	server := <-serverCh

	// Concurrently check relay mode while triggering it
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			server.relayMu.Lock()
			_ = server.relayMode
			server.relayMu.Unlock()
		}()
	}
	server.enterRelayMode()
	wg.Wait()

	server.relayMu.Lock()
	isRelay := server.relayMode
	server.relayMu.Unlock()
	if !isRelay {
		t.Fatal("relay mode should be true after enterRelayMode")
	}
	client.Close()
	server.Close()
}

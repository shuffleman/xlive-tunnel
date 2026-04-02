package rtmp

import (
	"net"
	"testing"
	"time"
)

func TestHandshakeClientServer(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	errCh := make(chan error, 2)
	go func() {
		c1b, err := writeC0C1(c1, time.Now())
		if err != nil {
			errCh <- err
			return
		}
		s1, err := readS0S1S2(c1)
		if err != nil {
			errCh <- err
			return
		}
		_ = c1b
		errCh <- writeC2(c1, s1)
	}()
	go func() {
		c1b, err := readC0C1(c2)
		if err != nil {
			errCh <- err
			return
		}
		_, err = writeS0S1S2(c2, time.Now(), c1b)
		if err != nil {
			errCh <- err
			return
		}
		errCh <- readC2(c2)
	}()
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
}

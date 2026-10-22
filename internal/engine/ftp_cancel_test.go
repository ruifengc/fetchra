package engine

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/RyanChen-cn/fetchra/internal/model"
)

func TestFTPCancelSilentGreeting(t *testing.T) {
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, e := l.Accept()
		if e == nil {
			accepted <- c
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	f := &FTP{}
	go func() {
		_, _, e := f.connect(ctx, model.Source{URL: "ftp://" + l.Addr().String() + "/file"})
		done <- e
	}()
	var connection net.Conn
	select {
	case connection = <-accepted:
		defer connection.Close()
	case <-time.After(time.Second):
		t.Fatal("client did not connect")
	}
	cancel()
	select {
	case e := <-done:
		if e == nil {
			t.Fatal("silent greeting unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("FTP cancellation hung on greeting")
	}
}

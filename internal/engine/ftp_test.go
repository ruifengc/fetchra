package engine

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/RyanChen-cn/fetchra/internal/model"
	"golang.org/x/time/rate"
)

type ftpFixture struct {
	listener net.Listener
	data     []byte
	rest     bool
	mu       sync.Mutex
	offsets  []int64
	wg       sync.WaitGroup
}

func newFTPFixture(t *testing.T, data []byte, rest bool) *ftpFixture {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	s := &ftpFixture{listener: l, data: data, rest: rest}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			c, e := l.Accept()
			if e != nil {
				return
			}
			s.wg.Add(1)
			go func() { defer s.wg.Done(); s.serve(c) }()
		}
	}()
	t.Cleanup(func() { l.Close(); s.wg.Wait() })
	return s
}
func (s *ftpFixture) serve(c net.Conn) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(15 * time.Second))
	send := func(text string) { fmt.Fprintf(c, "%s\r\n", text) }
	send("220 fixture")
	scanner := bufio.NewScanner(c)
	var dataListener net.Listener
	var offset int64
	defer func() {
		if dataListener != nil {
			dataListener.Close()
		}
	}()
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), " ", 2)
		cmd := parts[0]
		arg := ""
		if len(parts) > 1 {
			arg = parts[1]
		}
		switch cmd {
		case "USER":
			send("331 password needed")
		case "PASS":
			send("230 logged in")
		case "FEAT":
			features := "211-Features\r\n MDTM\r\n SIZE\r\n UTF8\r\n"
			if s.rest {
				features += " REST STREAM\r\n"
			}
			send(features + "211 End")
		case "OPTS", "TYPE", "NOOP":
			send("200 OK")
		case "SIZE":
			send(fmt.Sprintf("213 %d", len(s.data)))
		case "MDTM":
			send("213 20260102030405")
		case "EPSV":
			var e error
			dataListener, e = net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				return
			}
			port := dataListener.Addr().(*net.TCPAddr).Port
			send(fmt.Sprintf("229 Entering Extended Passive Mode (|||%d|)", port))
		case "REST":
			if !s.rest {
				send("502 unsupported")
			} else {
				fmt.Sscanf(arg, "%d", &offset)
				send("350 restart accepted")
			}
		case "RETR":
			if dataListener == nil {
				send("425 no data connection")
				continue
			}
			send("150 opening data")
			d, e := dataListener.Accept()
			dataListener.Close()
			dataListener = nil
			if e != nil {
				return
			}
			s.mu.Lock()
			s.offsets = append(s.offsets, offset)
			s.mu.Unlock()
			if offset < 0 || offset > int64(len(s.data)) {
				d.Close()
				send("550 bad offset")
				continue
			}
			_ = d.SetWriteDeadline(time.Now().Add(5 * time.Second))
			_, _ = io.Copy(d, bytes.NewReader(s.data[offset:]))
			d.Close()
			offset = 0
			send("226 transfer complete")
		case "QUIT":
			send("221 goodbye")
			return
		default:
			send("502 unsupported")
		}
	}
}
func TestFTPResumeAndFallback(t *testing.T) {
	for _, supports := range []bool{true, false} {
		t.Run(fmt.Sprint(supports), func(t *testing.T) {
			data := bytes.Repeat([]byte("ftpdata!"), int(BlockSize)/8+101)
			server := newFTPFixture(t, data, supports)
			f := &FTP{Credentials: map[string]model.Credential{}, Limit: rate.NewLimiter(rate.Inf, 64<<10)}
			source := model.Source{Protocol: "ftp", URL: "ftp://" + server.listener.Addr().String() + "/fixture.bin"}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			m, e := f.Probe(ctx, source)
			if e != nil {
				t.Fatal(e)
			}
			if m.Capabilities.Resume != supports {
				t.Fatalf("wrong capability: %+v", m.Capabilities)
			}
			task := model.Task{Source: source, Metadata: m, Dir: t.TempDir(), Policy: model.TaskPolicy{Connections: 1}}
			if supports {
				part, e := OpenPart(task.Dir)
				if e != nil {
					t.Fatal(e)
				}
				_, _ = part.Write(data[:BlockSize])
				part.Close()
				hash := sha256.Sum256(data[:BlockSize])
				c := Checkpoint{Source: m.Modified + ":" + fmt.Sprint(*m.Size), Size: m.Size, Blocks: map[int64]string{0: hex.EncodeToString(hash[:])}, Prefix: BlockSize}
				if e = SaveCheckpoint(task.Dir, c); e != nil {
					t.Fatal(e)
				}
			}
			var result model.File
			if e = f.Download(ctx, task, func(p Progress) {
				if len(p.Files) > 0 && p.Files[0].Ready {
					result = p.Files[0]
				}
			}); e != nil {
				t.Fatal(e)
			}
			if !result.Ready {
				t.Fatal("file not ready")
			}
			if supports {
				server.mu.Lock()
				found := false
				for _, off := range server.offsets {
					found = found || off == BlockSize
				}
				server.mu.Unlock()
				if !found {
					t.Fatal("did not resume from checkpoint")
				}
			}
		})
	}
}

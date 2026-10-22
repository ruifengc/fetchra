package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/RyanChen-cn/fetchra/internal/model"
	ftpclient "github.com/jlaffaye/ftp"
	"golang.org/x/time/rate"
)

type FTP struct {
	Credentials map[string]model.Credential
	Limit       *rate.Limiter
}

func (f *FTP) Close() error { return nil }
func (f *FTP) connect(ctx context.Context, s model.Source) (*ftpclient.ServerConn, string, error) {
	u, e := url.Parse(s.URL)
	if e != nil {
		return nil, "", model.Err("INVALID_ARGUMENT", "invalid FTP URL")
	}
	host := u.Host
	if u.Port() == "" {
		host = net.JoinHostPort(u.Hostname(), "21")
	}
	remote, e := url.PathUnescape(u.EscapedPath())
	if e != nil {
		return nil, "", model.Err("INVALID_ARGUMENT", "invalid FTP path")
	}
	if strings.ContainsAny(remote, "\r\n\x00") {
		return nil, "", model.Err("INVALID_ARGUMENT", "FTP path contains a control character")
	}
	dial := func(network, address string) (net.Conn, error) {
		conn, e := (&net.Dialer{Timeout: 15 * time.Second}).DialContext(ctx, network, address)
		if e != nil {
			return nil, e
		}
		wrapped := &ftpConn{Conn: conn, ctx: ctx}
		wrapped.stop = context.AfterFunc(ctx, func() { conn.Close() })
		return wrapped, nil
	}
	conn, e := ftpclient.Dial(host, ftpclient.DialWithDialFunc(dial), ftpclient.DialWithShutTimeout(5*time.Second))
	if e != nil {
		return nil, "", &model.Error{Code: "NETWORK_ERROR", Message: "FTP connection failed", Retryable: true}
	}
	user, pass := "anonymous", "fetchra@localhost"
	if s.CredentialRef != "" {
		c, ok := f.Credentials[s.CredentialRef]
		if !ok {
			_ = conn.Quit()
			return nil, "", model.Err("CREDENTIAL_NOT_FOUND", "credential reference is not configured")
		}
		user, pass = c.Username, c.Password
	}
	if strings.ContainsAny(user+pass, "\r\n\x00") {
		conn.Quit()
		return nil, "", model.Err("INVALID_ARGUMENT", "FTP credentials contain a control character")
	}
	if e = conn.Login(user, pass); e != nil {
		_ = conn.Quit()
		return nil, "", model.Err("AUTH_FAILED", "FTP login failed")
	}
	return conn, remote, nil
}
func (f *FTP) Probe(ctx context.Context, s model.Source) (*model.Metadata, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	conn, remote, e := f.connect(ctx, s)
	if e != nil {
		return nil, e
	}
	defer conn.Quit()
	var size *int64
	if n, e := conn.FileSize(remote); e == nil {
		size = &n
	}
	modified := ""
	if m, e := conn.GetTime(remote); e == nil {
		modified = m.UTC().Format(time.RFC3339Nano)
	}
	supports := false
	response, e := conn.RetrFrom(remote, 0)
	if e != nil {
		return nil, model.Err("NOT_FOUND", "FTP file cannot be retrieved")
	}
	_ = response.Close()
	// A non-zero restart proves REST acceptance. The response is immediately closed.
	if size != nil && *size > 0 {
		r, e := conn.RetrFrom(remote, uint64(*size-1))
		if e == nil {
			supports = true
			_ = r.Close()
		}
	}
	name := path.Base(remote)
	if !SafeOutputName(name) {
		name = "download"
	}
	caps := model.Capabilities{ResumeConfidence: "unsupported"}
	if supports && size != nil && modified != "" {
		caps.Resume = true
		caps.ResumeConfidence = "metadata_only"
	} else {
		caps.Reason = "FTP resume requires REST, SIZE and modification time"
	}
	return &model.Metadata{Files: []model.File{{Index: 0, Name: name, Size: size, Selected: true, Priority: "normal"}}, Capabilities: caps, Size: size, Modified: modified}, nil
}
func (f *FTP) Download(ctx context.Context, t model.Task, report Report) error {
	if e := PrepareDir(t.Dir); e != nil {
		return e
	}
	c, e := LoadCheckpoint(t.Dir)
	if e != nil {
		return e
	}
	report(Progress{Phase: "checking", Files: t.Metadata.Files})
	if file, e := RecoverComplete(ctx, t, c); e != nil {
		return e
	} else if file != nil {
		report(Progress{Files: []model.File{*file}})
		return nil
	}
	m, e := f.Probe(ctx, t.Source)
	if e != nil {
		return e
	}
	if differentSize(t.Metadata.Size, m.Size) || t.Metadata.Modified != m.Modified {
		return model.Err("SOURCE_CHANGED", "FTP source metadata changed; restart is required")
	}
	identity := m.Modified + ":"
	if m.Size != nil {
		identity += strconv.FormatInt(*m.Size, 10)
	}
	if c.Source != "" && c.Source != identity {
		return model.Err("SOURCE_CHANGED", "FTP checkpoint belongs to another version")
	}
	part, e := OpenPart(t.Dir)
	if e != nil {
		return model.Err("DISK_ERROR", "cannot open FTP partial file")
	}
	defer part.Close()
	if e = VerifyBlocks(ctx, part, &c); e != nil {
		return e
	}
	var offset int64
	for {
		if _, ok := c.Blocks[offset/BlockSize]; !ok {
			break
		}
		n := BlockSize
		if m.Size != nil && *m.Size-offset < n {
			n = *m.Size - offset
		}
		if n <= 0 {
			break
		}
		offset += n
		if m.Size != nil && offset == *m.Size {
			break
		}
	}
	info, e := part.Stat()
	if e != nil {
		return e
	}
	if info.Size() > 0 && !m.Capabilities.Resume {
		return model.Err("RESUME_UNSAFE", "FTP server cannot validate partial data; restart is required")
	}
	if e = part.Truncate(offset); e != nil {
		return model.Err("DISK_ERROR", "cannot truncate FTP partial file")
	}
	c.Source = identity
	c.Size = m.Size
	c.Prefix = offset
	if m.Size != nil && offset == *m.Size {
		file, e := CompleteFile(ctx, t, part, &c)
		if e != nil {
			return e
		}
		report(Progress{Files: []model.File{file}})
		return nil
	}
	conn, remote, e := f.connect(ctx, t.Source)
	if e != nil {
		return e
	}
	defer conn.Quit()
	response, e := conn.RetrFrom(remote, uint64(offset))
	if e != nil {
		return model.Err("RESUME_UNSUPPORTED", "FTP server rejected retrieval offset")
	}
	defer response.Close()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = response.SetDeadline(time.Now())
		case <-done:
		}
	}()
	initial := t.Metadata.Files[0]
	initial.Completed = offset
	report(Progress{Phase: "downloading", Files: []model.File{initial}})
	buf := make([]byte, BlockSize)
	var transferred int64
	lastReport := time.Now()
	reader := countingReader{LimitedReader{ctx, ftpIdleReader{ctx, response}, f.Limit}, func(n int64) {
		transferred += n
		if time.Since(lastReport) >= 250*time.Millisecond {
			file := t.Metadata.Files[0]
			file.Completed = offset
			report(Progress{Phase: "downloading", Files: []model.File{file}, Transferred: transferred})
			lastReport = time.Now()
		}
	}}
	for {
		n, readErr := io.ReadFull(reader, buf)
		if n > 0 {
			if _, e = part.WriteAt(buf[:n], offset); e != nil {
				return model.Err("DISK_ERROR", "cannot write FTP data")
			}
			if e = part.Sync(); e != nil {
				return model.Err("DISK_ERROR", "cannot sync FTP data")
			}
			hash := sha256.Sum256(buf[:n])
			if readErr == nil || (m.Size != nil && offset+int64(n) == *m.Size) {
				c.Blocks[offset/BlockSize] = hex.EncodeToString(hash[:])
			}
			offset += int64(n)
			c.Prefix = offset
			if e = SaveCheckpoint(t.Dir, c); e != nil {
				return e
			}
			file := t.Metadata.Files[0]
			file.Completed = offset
			report(Progress{Phase: "downloading", Files: []model.File{file}, Transferred: transferred})
		}
		if readErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
				break
			}
			return &model.Error{Code: "NETWORK_ERROR", Message: "FTP data connection failed", Retryable: true}
		}
	}
	if m.Size != nil && offset != *m.Size {
		return &model.Error{Code: "INCOMPLETE_RESPONSE", Message: "FTP file was truncated", Retryable: true}
	}
	size := offset
	c.Size = &size
	file, e := CompleteFile(ctx, t, part, &c)
	if e != nil {
		return e
	}
	report(Progress{Files: []model.File{file}})
	return nil
}

type ftpIdleReader struct {
	ctx      context.Context
	response *ftpclient.Response
}

type ftpConn struct {
	net.Conn
	ctx  context.Context
	stop func() bool
}

func (c *ftpConn) Read(p []byte) (int, error) {
	if e := c.ctx.Err(); e != nil {
		return 0, e
	}
	_ = c.Conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	return c.Conn.Read(p)
}
func (c *ftpConn) Write(p []byte) (int, error) {
	if e := c.ctx.Err(); e != nil {
		return 0, e
	}
	_ = c.Conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	return c.Conn.Write(p)
}
func (c *ftpConn) Close() error {
	if c.stop != nil {
		c.stop()
	}
	return c.Conn.Close()
}

func (r ftpIdleReader) Read(p []byte) (int, error) {
	_ = r.response.SetDeadline(time.Now().Add(30 * time.Second))
	if e := r.ctx.Err(); e != nil {
		return 0, e
	}
	return r.response.Read(p)
}

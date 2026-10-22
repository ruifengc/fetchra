package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RyanChen-cn/fetchra/internal/model"
	"golang.org/x/time/rate"
)

type HTTP struct {
	Transport     http.RoundTripper
	NoRedirect    bool
	transportOnce sync.Once
	transport     http.RoundTripper
	Credentials   map[string]model.Credential
	Limit         *rate.Limiter
	Hosts         *Hosts
}

func (h *HTTP) Close() error {
	if tr, ok := h.transport.(interface{ CloseIdleConnections() }); ok {
		tr.CloseIdleConnections()
	}
	return nil
}
func (h *HTTP) client() *http.Client {
	h.transportOnce.Do(func() {
		h.transport = h.Transport
		if h.transport == nil {
			h.transport = &http.Transport{Proxy: http.ProxyFromEnvironment, DialContext: (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext, ForceAttemptHTTP2: true, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 15 * time.Second, DisableCompression: true, MaxIdleConns: 64, MaxIdleConnsPerHost: 8}
		}
	})
	return &http.Client{Transport: h.transport, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if h.NoRedirect {
			return model.Err("PROTOCOL_ERROR", "node file redirects are not allowed")
		}
		if len(via) >= 10 {
			return model.Err("TOO_MANY_REDIRECTS", "redirect limit exceeded")
		}
		if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
			return model.Err("UNSUPPORTED_PROTOCOL", "HTTP redirect must target HTTP or HTTPS")
		}
		if len(via) > 0 && (req.URL.Scheme != via[0].URL.Scheme || req.URL.Host != via[0].URL.Host) {
			req.Header = make(http.Header)
			req.Header.Set("Accept-Encoding", "identity")
		}
		return nil
	}}
}
func (h *HTTP) request(ctx context.Context, s model.Source, method string) (*http.Request, error) {
	r, e := http.NewRequestWithContext(ctx, method, s.URL, nil)
	if e != nil {
		return nil, model.Err("INVALID_ARGUMENT", "invalid HTTP URL")
	}
	if s.CredentialRef != "" {
		cred, ok := h.Credentials[s.CredentialRef]
		if !ok {
			return nil, model.Err("CREDENTIAL_NOT_FOUND", "credential reference is not configured")
		}
		for k, v := range cred.Headers {
			switch strings.ToLower(k) {
			case "range", "if-range", "accept-encoding", "host", "content-length", "connection":
				return nil, model.Err("INVALID_ARGUMENT", "credential headers cannot override transfer control headers")
			}
			r.Header.Set(k, v)
		}
		if cred.Username != "" {
			r.SetBasicAuth(cred.Username, cred.Password)
		}
	}
	r.Header.Set("Accept-Encoding", "identity")
	return r, nil
}
func strongETag(v string) bool { return len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' }
func responseError(code int) error {
	switch {
	case code == 401 || code == 403:
		return model.Err("AUTH_FAILED", "server rejected access")
	case code == 404:
		return model.Err("NOT_FOUND", "remote file was not found")
	case code == 429 || code >= 500:
		return &model.Error{Code: "REMOTE_UNAVAILABLE", Message: "server temporarily rejected the request", Retryable: true}
	default:
		return model.Err("HTTP_STATUS", fmt.Sprintf("unexpected HTTP status %d", code))
	}
}
func httpError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var me *model.Error
	if errors.As(err, &me) {
		return me
	}
	return &model.Error{Code: "NETWORK_ERROR", Message: "HTTP connection failed", Retryable: true}
}
func (h *HTTP) Probe(ctx context.Context, s model.Source) (*model.Metadata, error) {
	client := h.client()
	probeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	r, e := h.request(probeCtx, s, "HEAD")
	if e != nil {
		return nil, e
	}
	resp, e := client.Do(r)
	if e != nil {
		return nil, httpError(ctx, e)
	}
	if resp.StatusCode == 401 || resp.StatusCode == 403 || resp.StatusCode == 404 {
		defer resp.Body.Close()
		return nil, responseError(resp.StatusCode)
	}
	// Always test a real range. HEAD and Accept-Ranges alone do not prove support.
	resp.Body.Close()
	r, e = h.request(probeCtx, s, "GET")
	if e != nil {
		return nil, e
	}
	r.Header.Set("Range", "bytes=0-0")
	resp, e = client.Do(r)
	if e != nil {
		return nil, httpError(ctx, e)
	}
	defer resp.Body.Close()
	var size *int64
	supports := false
	switch resp.StatusCode {
	case http.StatusPartialContent:
		begin, end, total, err := parseRange(resp.Header.Get("Content-Range"))
		if err != nil || begin != 0 || end != 0 {
			return nil, model.Err("PROTOCOL_ERROR", "invalid range probe response")
		}
		size = &total
		supports = true
	case http.StatusOK:
		if resp.ContentLength >= 0 {
			n := resp.ContentLength
			size = &n
		}
	case http.StatusRequestedRangeNotSatisfiable:
		if resp.Header.Get("Content-Range") == "bytes */0" {
			n := int64(0)
			size = &n
		} else {
			return nil, responseError(resp.StatusCode)
		}
	default:
		return nil, responseError(resp.StatusCode)
	}
	if enc := resp.Header.Get("Content-Encoding"); enc != "" && enc != "identity" {
		supports = false
		size = nil
	}
	name := path.Base(resp.Request.URL.Path)
	if decoded, e := url.PathUnescape(name); e == nil {
		name = decoded
	}
	if _, params, e := mime.ParseMediaType(resp.Header.Get("Content-Disposition")); e == nil && params["filename"] != "" {
		name = params["filename"]
	}
	if !SafeOutputName(name) {
		name = "download"
	}
	etag := resp.Header.Get("ETag")
	caps := model.Capabilities{ResumeConfidence: "unsupported"}
	if supports && strongETag(etag) && size != nil {
		caps.Resume = true
		caps.ResumeConfidence = "strong_validator"
		caps.Parallel = true
	} else {
		caps.Reason = "single stream: safe range download requires a known size, range support and a strong ETag"
	}
	return &model.Metadata{Files: []model.File{{Index: 0, Name: name, Size: size, Selected: true, Priority: "normal"}}, Size: size, Capabilities: caps, ETag: etag, Modified: resp.Header.Get("Last-Modified"), FinalURL: resp.Request.URL.String()}, nil
}
func parseRange(v string) (int64, int64, int64, error) {
	if !strings.HasPrefix(v, "bytes ") {
		return 0, 0, 0, fmt.Errorf("invalid range")
	}
	parts := strings.Split(strings.TrimPrefix(v, "bytes "), "/")
	if len(parts) != 2 {
		return 0, 0, 0, fmt.Errorf("invalid range")
	}
	ends := strings.Split(parts[0], "-")
	if len(ends) != 2 {
		return 0, 0, 0, fmt.Errorf("invalid range")
	}
	a, e1 := strconv.ParseInt(ends[0], 10, 64)
	b, e2 := strconv.ParseInt(ends[1], 10, 64)
	n, e3 := strconv.ParseInt(parts[1], 10, 64)
	if e1 != nil || e2 != nil || e3 != nil || a < 0 || b < a || n <= b {
		return 0, 0, 0, fmt.Errorf("invalid range")
	}
	return a, b, n, nil
}
func (h *HTTP) Download(ctx context.Context, t model.Task, report Report) error {
	if e := PrepareDir(t.Dir); e != nil {
		return e
	}
	c, e := LoadCheckpoint(t.Dir)
	if e != nil {
		return e
	}
	report(Progress{Phase: "checking", Files: t.Metadata.Files})
	if f, e := RecoverComplete(ctx, t, c); e != nil {
		return e
	} else if f != nil {
		report(Progress{Files: []model.File{*f}})
		return nil
	}
	m, e := h.Probe(ctx, t.Source)
	if e != nil {
		return e
	}
	if (t.Metadata.ETag != "" && m.ETag != t.Metadata.ETag) || differentSize(t.Metadata.Size, m.Size) {
		return model.Err("SOURCE_CHANGED", "remote file changed; restart is required")
	}
	part, e := OpenPart(t.Dir)
	if e != nil {
		return model.Err("DISK_ERROR", "cannot open partial download")
	}
	defer part.Close()
	if c.Source != "" && c.Source != m.ETag {
		return model.Err("SOURCE_CHANGED", "checkpoint belongs to another remote version")
	}
	if e = VerifyBlocks(ctx, part, &c); e != nil {
		return model.Err("DISK_ERROR", "cannot verify checkpoint")
	}
	info, e := part.Stat()
	if e != nil {
		return e
	}
	if !m.Capabilities.Resume && info.Size() > 0 {
		return model.Err("RESUME_UNSAFE", "server cannot validate a partial download; restart is required")
	}
	c.Source = m.ETag
	c.Size = m.Size
	t.Metadata.Capabilities = m.Capabilities
	if m.Capabilities.Resume {
		e = h.ranges(ctx, t, part, &c, report)
	} else {
		e = h.single(ctx, t, part, &c, report)
	}
	if e != nil {
		return e
	}
	if e = ctx.Err(); e != nil {
		return e
	}
	if c.Size != nil {
		if e = part.Truncate(*c.Size); e != nil {
			return model.Err("DISK_ERROR", "cannot set final file length")
		}
	}
	file, e := CompleteFile(ctx, t, part, &c)
	if e != nil {
		return e
	}
	report(Progress{Files: []model.File{file}})
	return nil
}
func differentSize(a, b *int64) bool { return a != nil && b != nil && *a != *b }
func (h *HTTP) ranges(ctx context.Context, t model.Task, f *os.File, c *Checkpoint, report Report) error {
	if c.Size == nil {
		return model.Err("PROTOCOL_ERROR", "range transfer requires a known size")
	}
	total := *c.Size
	count := (total + BlockSize - 1) / BlockSize
	workers := t.Policy.Connections
	if workers < 1 {
		workers = 1
	}
	if workers > 4 {
		workers = 4
	}
	if total < 16<<20 {
		workers = 1
	}
	if int64(workers) > count {
		workers = int(count)
	}
	client := h.client()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int64)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var first error
	var transferred atomic.Int64
	publish := func() {
		var complete int64
		for block := range c.Blocks {
			length := BlockSize
			if total-block*BlockSize < length {
				length = total - block*BlockSize
			}
			complete += length
		}
		file := t.Metadata.Files[0]
		file.Completed = complete
		report(Progress{Phase: "downloading", Files: []model.File{file}, Transferred: transferred.Load()})
	}
	pending := make([]int64, 0)
	for block := int64(0); block < count; block++ {
		if _, ok := c.Blocks[block]; !ok {
			pending = append(pending, block)
		}
	}
	mu.Lock()
	publish()
	mu.Unlock()
	tickDone := make(chan struct{})
	tickFinished := make(chan struct{})
	go func() {
		defer close(tickFinished)
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-tickDone:
				return
			case <-ticker.C:
				mu.Lock()
				publish()
				mu.Unlock()
			}
		}
	}()
	defer func() { close(tickDone); <-tickFinished }()
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buffer := make([]byte, int(BlockSize)+1)
			for block := range jobs {
				start := block * BlockSize
				length := BlockSize
				if total-start < length {
					length = total - start
				}
				data, err := h.readRange(runCtx, client, t, start, length, total, c.Source, &transferred, buffer)
				mu.Lock()
				if err == nil {
					if _, err = f.WriteAt(data, start); err == nil {
						err = f.Sync()
					}
					if err == nil {
						hash := sha256.Sum256(data)
						c.Blocks[block] = hex.EncodeToString(hash[:])
						err = SaveCheckpoint(t.Dir, *c)
					}
					if err != nil {
						err = model.Err("DISK_ERROR", "cannot commit downloaded block")
					}
				}
				if err != nil {
					if first == nil {
						first = err
						cancel()
					}
					mu.Unlock()
					return
				}
				publish()
				mu.Unlock()
			}
		}()
	}
send:
	for _, block := range pending {
		select {
		case jobs <- block:
		case <-runCtx.Done():
			break send
		}
	}
	close(jobs)
	wg.Wait()
	if first != nil {
		return first
	}
	return ctx.Err()
}
func (h *HTTP) readRange(ctx context.Context, client *http.Client, t model.Task, start, length, total int64, etag string, transferred *atomic.Int64, buffer []byte) ([]byte, error) {
	u, _ := url.Parse(t.Metadata.FinalURL)
	if u == nil || u.Host == "" {
		u, _ = url.Parse(t.Source.URL)
	}
	release, e := h.Hosts.Acquire(ctx, u.Scheme+"://"+u.Host)
	if e != nil {
		return nil, e
	}
	defer release()
	requestCtx, cancelRequest := context.WithCancel(ctx)
	defer cancelRequest()
	r, e := h.request(requestCtx, t.Source, "GET")
	if e != nil {
		return nil, e
	}
	r.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, start+length-1))
	r.Header.Set("If-Range", etag)
	resp, e := client.Do(r)
	if e != nil {
		return nil, httpError(ctx, e)
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		return nil, model.Err("RANGE_CHANGED", "server ignored a validated range; restart as a single stream")
	}
	if resp.StatusCode != 206 {
		return nil, responseError(resp.StatusCode)
	}
	a, b, n, e := parseRange(resp.Header.Get("Content-Range"))
	if e != nil || a != start || b != start+length-1 || n != total {
		return nil, model.Err("PROTOCOL_ERROR", "range response does not match the requested interval")
	}
	if v := resp.Header.Get("ETag"); v != "" && v != etag {
		return nil, model.Err("SOURCE_CHANGED", "ETag changed during range transfer")
	}
	if enc := resp.Header.Get("Content-Encoding"); enc != "" && enc != "identity" {
		return nil, model.Err("PROTOCOL_ERROR", "compressed range response is unsupported")
	}
	if int64(len(buffer)) < length+1 {
		buffer = make([]byte, length+1)
	}
	reader := countingReader{LimitedReader{ctx, idleReader{resp.Body, cancelRequest}, h.Limit}, func(n int64) { transferred.Add(n) }}
	received, e := io.ReadFull(reader, buffer[:length+1])
	if e != nil && e != io.EOF && e != io.ErrUnexpectedEOF {
		return nil, httpError(ctx, e)
	}
	if int64(received) > length {
		return nil, model.Err("PROTOCOL_ERROR", "range response exceeded the requested interval")
	}
	if int64(received) != length {
		return nil, &model.Error{Code: "INCOMPLETE_RESPONSE", Message: "range response was truncated", Retryable: true}
	}
	return buffer[:received], nil
}
func (h *HTTP) single(ctx context.Context, t model.Task, f *os.File, c *Checkpoint, report Report) error {
	client := h.client()
	u, _ := url.Parse(t.Source.URL)
	release, e := h.Hosts.Acquire(ctx, u.Scheme+"://"+u.Host)
	if e != nil {
		return e
	}
	defer release()
	requestCtx, cancelRequest := context.WithCancel(ctx)
	defer cancelRequest()
	r, e := h.request(requestCtx, t.Source, "GET")
	if e != nil {
		return e
	}
	resp, e := client.Do(r)
	if e != nil {
		return httpError(ctx, e)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return responseError(resp.StatusCode)
	}
	if t.Metadata.ETag != "" && resp.Header.Get("ETag") != t.Metadata.ETag {
		return model.Err("SOURCE_CHANGED", "ETag changed before transfer")
	}
	if c.Size != nil && resp.ContentLength >= 0 && *c.Size != resp.ContentLength {
		return model.Err("SOURCE_CHANGED", "remote length changed before transfer")
	}
	if enc := resp.Header.Get("Content-Encoding"); enc != "" && enc != "identity" {
		return model.Err("PROTOCOL_ERROR", "server did not honor identity encoding")
	}
	if e = f.Truncate(0); e != nil {
		return model.Err("DISK_ERROR", "cannot reset partial file")
	}
	report(Progress{Phase: "downloading", Files: t.Metadata.Files})
	buf := make([]byte, BlockSize)
	var offset int64
	var transferred int64
	lastReport := time.Now()
	reader := countingReader{LimitedReader{ctx, idleReader{resp.Body, cancelRequest}, h.Limit}, func(n int64) {
		transferred += n
		if time.Since(lastReport) >= 250*time.Millisecond {
			file := t.Metadata.Files[0]
			file.Completed = offset
			report(Progress{Phase: "downloading", Files: []model.File{file}, Transferred: transferred})
			lastReport = time.Now()
		}
	}}
	for {
		n, e := io.ReadFull(reader, buf)
		if n > 0 {
			if _, err := f.WriteAt(buf[:n], offset); err != nil {
				return model.Err("DISK_ERROR", "cannot write partial file")
			}
			offset += int64(n)
			if err := f.Sync(); err != nil {
				return model.Err("DISK_ERROR", "cannot sync partial file")
			}
			c.Prefix = offset
			if e == nil {
				hash := sha256.Sum256(buf[:n])
				c.Blocks[(offset-int64(n))/BlockSize] = hex.EncodeToString(hash[:])
			}
			if err := SaveCheckpoint(t.Dir, *c); err != nil {
				return err
			}
			file := t.Metadata.Files[0]
			file.Completed = offset
			report(Progress{Phase: "downloading", Files: []model.File{file}, Transferred: transferred})
		}
		if e != nil {
			if e == io.EOF || e == io.ErrUnexpectedEOF {
				break
			}
			return httpError(ctx, e)
		}
	}
	if c.Size != nil && offset != *c.Size {
		return &model.Error{Code: "INCOMPLETE_RESPONSE", Message: "stream was shorter than the expected length", Retryable: true}
	}
	size := offset
	c.Size = &size
	return nil
}

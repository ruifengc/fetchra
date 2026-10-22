package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RyanChen-cn/fetchra/internal/model"
	"golang.org/x/time/rate"
)

func testHTTP() *HTTP {
	return &HTTP{Limit: rate.NewLimiter(rate.Inf, 64<<10), Hosts: &Hosts{limit: 4, used: map[string]int{}}, Credentials: map[string]model.Credential{}}
}

type rangeServer struct {
	data    []byte
	etag    string
	ignore  bool
	mu      sync.Mutex
	starts  []int64
	active  atomic.Int32
	maximum atomic.Int32
}

func (s *rangeServer) handler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	etag := s.etag
	s.mu.Unlock()
	if etag != "" {
		w.Header().Set("ETag", etag)
	}
	w.Header().Set("Content-Disposition", `attachment; filename="fixture.bin"`)
	if r.Method == "HEAD" {
		w.Header().Set("Content-Length", strconv.Itoa(len(s.data)))
		return
	}
	start, end := int64(0), int64(len(s.data)-1)
	rangeRequest := r.Header.Get("Range") != "" && !s.ignore
	if rangeRequest {
		if _, e := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); e != nil || start < 0 || end < start || end >= int64(len(s.data)) {
			w.WriteHeader(416)
			return
		}
		if v := r.Header.Get("If-Range"); v != "" && v != etag {
			rangeRequest = false
			start = 0
			end = int64(len(s.data) - 1)
		}
	}
	if rangeRequest {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(s.data)))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(206)
	} else {
		w.Header().Set("Content-Length", strconv.Itoa(len(s.data)))
	}
	if end-start > 0 {
		a := s.active.Add(1)
		defer s.active.Add(-1)
		for {
			old := s.maximum.Load()
			if old >= a || s.maximum.CompareAndSwap(old, a) {
				break
			}
		}
		s.mu.Lock()
		s.starts = append(s.starts, start)
		s.mu.Unlock()
		time.Sleep(30 * time.Millisecond)
	}
	_, _ = w.Write(s.data[start : end+1])
}
func preparedHTTP(t *testing.T, h *HTTP, source string) model.Task {
	t.Helper()
	m, e := h.Probe(context.Background(), model.Source{Protocol: "http", URL: source})
	if e != nil {
		t.Fatal(e)
	}
	return model.Task{ID: "test", Source: model.Source{Protocol: "http", URL: source}, Dir: t.TempDir(), Metadata: m, Policy: model.TaskPolicy{Priority: "normal", Connections: 4}}
}
func TestHTTPParallelAndIntegrity(t *testing.T) {
	data := bytes.Repeat([]byte("abcdefgh"), 3*int(BlockSize)/8+101)
	remote := &rangeServer{data: data, etag: `"version1"`}
	server := httptest.NewServer(http.HandlerFunc(remote.handler))
	defer server.Close()
	h := testHTTP()
	task := preparedHTTP(t, h, server.URL+"/file")
	sum := sha256.Sum256(data)
	task.SHA256 = hex.EncodeToString(sum[:])
	var result model.File
	if e := h.Download(context.Background(), task, func(p Progress) {
		if len(p.Files) > 0 && p.Files[0].Ready {
			result = p.Files[0]
		}
	}); e != nil {
		t.Fatal(e)
	}
	if !result.Ready || result.Integrity != "sha256_verified" {
		t.Fatalf("bad result: %+v", result)
	}
	got, e := os.ReadFile(result.Path)
	if e != nil || !bytes.Equal(got, data) {
		t.Fatal("published data mismatch", e)
	}
	if remote.maximum.Load() < 2 || remote.maximum.Load() > 4 {
		t.Fatalf("unexpected concurrency %d", remote.maximum.Load())
	}
	// Completed files are locally checked without contacting the source again.
	server.Close()
	if e := h.Download(context.Background(), task, func(Progress) {}); e != nil {
		t.Fatal(e)
	}
}
func TestHTTPResumeRechecksLocalBlocks(t *testing.T) {
	data := bytes.Repeat([]byte("resume!!"), 3*int(BlockSize)/8+7)
	remote := &rangeServer{data: data, etag: `"same"`}
	server := httptest.NewServer(http.HandlerFunc(remote.handler))
	defer server.Close()
	h := testHTTP()
	task := preparedHTTP(t, h, server.URL)
	f, e := OpenPart(task.Dir)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.Write(data[:2*BlockSize]); e != nil {
		t.Fatal(e)
	}
	_, _ = f.WriteAt([]byte("damage"), BlockSize)
	f.Close()
	c := Checkpoint{Source: `"same"`, Size: task.Metadata.Size, Blocks: map[int64]string{}}
	for i := int64(0); i < 2; i++ {
		hash := sha256.Sum256(data[i*BlockSize : (i+1)*BlockSize])
		c.Blocks[i] = hex.EncodeToString(hash[:])
	}
	if e = SaveCheckpoint(task.Dir, c); e != nil {
		t.Fatal(e)
	}
	if e = h.Download(context.Background(), task, func(Progress) {}); e != nil {
		t.Fatal(e)
	}
	remote.mu.Lock()
	defer remote.mu.Unlock()
	for _, start := range remote.starts {
		if start == 0 {
			t.Fatal("valid first block was downloaded again")
		}
	}
	if len(remote.starts) != 3 {
		t.Fatalf("expected damaged and missing blocks, got %v", remote.starts)
	}
}
func TestHTTPUnsafeResumeAndSourceChange(t *testing.T) {
	for _, etag := range []string{"", `"old"`} {
		t.Run(etag, func(t *testing.T) {
			data := bytes.Repeat([]byte("x"), 1024)
			remote := &rangeServer{data: data, etag: etag}
			server := httptest.NewServer(http.HandlerFunc(remote.handler))
			defer server.Close()
			h := testHTTP()
			task := preparedHTTP(t, h, server.URL)
			f, e := OpenPart(task.Dir)
			if e != nil {
				t.Fatal(e)
			}
			_, _ = f.Write([]byte("partial"))
			f.Close()
			want := "RESUME_UNSAFE"
			if etag != "" {
				remote.mu.Lock()
				remote.etag = `"new"`
				remote.mu.Unlock()
				want = "SOURCE_CHANGED"
			}
			e = h.Download(context.Background(), task, func(Progress) {})
			if me, ok := e.(*model.Error); !ok || me.Code != want {
				t.Fatalf("want %s, got %v", want, e)
			}
		})
	}
}
func TestHTTPRangeFallbackAndChecksumFailure(t *testing.T) {
	data := []byte("a simple single stream")
	remote := &rangeServer{data: data, etag: `"v"`, ignore: true}
	server := httptest.NewServer(http.HandlerFunc(remote.handler))
	defer server.Close()
	h := testHTTP()
	task := preparedHTTP(t, h, server.URL)
	if task.Metadata.Capabilities.Parallel || task.Metadata.Capabilities.Resume {
		t.Fatal("server ignoring range must be single stream")
	}
	task.SHA256 = strings.Repeat("0", 64)
	e := h.Download(context.Background(), task, func(Progress) {})
	if me, ok := e.(*model.Error); !ok || me.Code != "CHECKSUM_MISMATCH" {
		t.Fatal(e)
	}
	if _, e = os.Stat(filepath.Join(task.Dir, "fixture.bin")); !os.IsNotExist(e) {
		t.Fatal("invalid content was published")
	}
}
func TestHTTPUnknownSizeAndRedirectCredentials(t *testing.T) {
	var leaked atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" || r.Header.Get("X-Secret") != "" {
			leaked.Store(true)
		}
		if r.Method == "HEAD" {
			return
		}
		w.(http.Flusher).Flush()
		_, _ = w.Write([]byte("unknown length"))
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL+"/output", 302) }))
	defer redirect.Close()
	h := testHTTP()
	h.Credentials["secret"] = model.Credential{Headers: map[string]string{"Authorization": "Bearer secret", "X-Secret": "private"}}
	source := model.Source{Protocol: "http", URL: redirect.URL, CredentialRef: "secret"}
	m, e := h.Probe(context.Background(), source)
	if e != nil {
		t.Fatal(e)
	}
	if m.Size != nil {
		t.Fatal("unknown size should be nil")
	}
	task := model.Task{Source: source, Metadata: m, Dir: t.TempDir(), Policy: model.TaskPolicy{Connections: 4}}
	if e = h.Download(context.Background(), task, func(Progress) {}); e != nil {
		t.Fatal(e)
	}
	if leaked.Load() {
		t.Fatal("credentials leaked across origin redirect")
	}
}
func TestRootConfinementAndPublication(t *testing.T) {
	dir := t.TempDir()
	outside := t.TempDir()
	if e := os.Symlink(filepath.Join(outside, "victim"), filepath.Join(dir, "content.part")); e != nil {
		t.Fatal(e)
	}
	if f, e := OpenPart(dir); e == nil {
		f.Close()
		t.Fatal("followed out-of-root symlink")
	}
	if _, e := os.Stat(filepath.Join(outside, "victim")); !os.IsNotExist(e) {
		t.Fatal("created outside file")
	}
}

func TestHTTPTransientRetryRecoversCommittedBlocks(t *testing.T) {
	data := bytes.Repeat([]byte("r"), 2*int(BlockSize)+1)
	remote := &rangeServer{data: data, etag: `"retry"`}
	var rejected atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.Header.Get("Range"), fmt.Sprintf("bytes=%d-", BlockSize)) && rejected.CompareAndSwap(false, true) {
			w.WriteHeader(503)
			return
		}
		remote.handler(w, r)
	}))
	defer server.Close()
	h := testHTTP()
	defer h.Close()
	task := preparedHTTP(t, h, server.URL+"/retry")
	task.Policy.Connections = 1
	var completed model.File
	if err := RetryDownload(context.Background(), h, task, func(p Progress) {
		if len(p.Files) > 0 && p.Files[0].Ready {
			completed = p.Files[0]
		}
	}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(completed.Path)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatal("retried download is corrupt", err)
	}
	remote.mu.Lock()
	defer remote.mu.Unlock()
	zeros := 0
	for _, start := range remote.starts {
		if start == 0 {
			zeros++
		}
	}
	if !rejected.Load() || zeros != 1 {
		t.Fatal("retry re-downloaded the committed first block", remote.starts)
	}
}

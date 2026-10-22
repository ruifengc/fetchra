package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/RyanChen-cn/fetchra/internal/model"
	"golang.org/x/time/rate"
)

const BlockSize int64 = 8 << 20

type Progress struct {
	Phase       string
	Files       []model.File
	Transferred int64
	Seeding     bool
}
type Report func(Progress)

// Engines own protocol-specific probing and recovery. Cancelling Download pauses
// a task; snapshots are pushed through Report, never polled through library types.
type Engine interface {
	Probe(context.Context, model.Source) (*model.Metadata, error)
	Download(context.Context, model.Task, Report) error
	Close() error
}
type Registry struct {
	Engines       map[string]Engine
	DownloadLimit *rate.Limiter
	UploadLimit   *rate.Limiter
	Hosts         *Hosts
}

func NewRegistry(root string, credentials map[string]model.Credential) (*Registry, error) {
	dl := rate.NewLimiter(rate.Inf, 64<<10)
	ul := rate.NewLimiter(rate.Inf, 64<<10)
	hosts := &Hosts{limit: 4, used: map[string]int{}}
	httpEngine := &HTTP{Credentials: credentials, Limit: dl, Hosts: hosts}
	bt, err := NewTorrent(root, dl, ul)
	if err != nil {
		return nil, err
	}
	return &Registry{map[string]Engine{"http": httpEngine, "ftp": &FTP{Credentials: credentials, Limit: dl}, "torrent": bt}, dl, ul, hosts}, nil
}
func (r *Registry) SetPolicy(p model.Policy) {
	for _, v := range []struct {
		l *rate.Limiter
		n int64
	}{{r.DownloadLimit, p.DownloadRate}, {r.UploadLimit, p.UploadRate}} {
		limit := rate.Inf
		if v.n > 0 {
			limit = rate.Limit(v.n)
		}
		v.l.SetLimit(limit)
	}
	r.Hosts.SetLimit(p.PerHost)
}
func (r *Registry) Close() error {
	var first error
	for _, e := range r.Engines {
		if err := e.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func NewHosts(limit int) *Hosts { return &Hosts{limit: limit, used: map[string]int{}} }

type Hosts struct {
	mu      sync.Mutex
	limit   int
	used    map[string]int
	waiting map[string]chan struct{}
}

func (h *Hosts) wake(key string) {
	if ch := h.waiting[key]; ch != nil {
		close(ch)
		delete(h.waiting, key)
	}
}
func (h *Hosts) SetLimit(limit int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.limit = limit
	for key := range h.waiting {
		h.wake(key)
	}
}
func (h *Hosts) Acquire(ctx context.Context, key string) (func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		h.mu.Lock()
		if h.used[key] < h.limit {
			h.used[key]++
			h.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() {
					h.mu.Lock()
					defer h.mu.Unlock()
					h.used[key]--
					if h.used[key] == 0 {
						delete(h.used, key)
					}
					if h.used[key] < h.limit {
						h.wake(key)
					}
				})
			}, nil
		}
		if h.waiting == nil {
			h.waiting = map[string]chan struct{}{}
		}
		ch := h.waiting[key]
		if ch == nil {
			ch = make(chan struct{})
			h.waiting[key] = ch
		}
		h.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch:
		}
	}
}

func SafeName(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsAny(name, "/\\\x00")
}
func SafeOutputName(name string) bool {
	return SafeName(name) && name != "content.part" && name != "checkpoint.json" && name != "checkpoint.tmp"
}
func PrepareDir(dir string) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return model.Err("DISK_ERROR", "cannot create task directory")
	}
	info, err := os.Lstat(dir)
	if err != nil || info.Mode()&os.ModeSymlink != 0 {
		return model.Err("INVALID_PATH", "task directory must not be a symbolic link")
	}
	return nil
}

type Checkpoint struct {
	Source   string           `json:"source"`
	Size     *int64           `json:"size"`
	Blocks   map[int64]string `json:"blocks"`
	Prefix   int64            `json:"prefix"`
	Complete bool             `json:"complete"`
	Hash     string           `json:"hash,omitempty"`
	Output   string           `json:"output,omitempty"`
}

func LoadCheckpoint(dir string) (Checkpoint, error) {
	c := Checkpoint{Blocks: map[int64]string{}}
	r, e := os.OpenRoot(dir)
	if e != nil {
		return c, e
	}
	defer r.Close()
	f, e := r.Open("checkpoint.json")
	if os.IsNotExist(e) {
		return c, nil
	}
	if e != nil {
		return c, model.Err("DISK_ERROR", "cannot read checkpoint")
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, 16<<20))
	if os.IsNotExist(e) {
		return c, nil
	}
	if e != nil {
		return c, model.Err("DISK_ERROR", "cannot read checkpoint")
	}
	if json.Unmarshal(b, &c) != nil || c.Blocks == nil {
		return c, model.Err("CHECKPOINT_CORRUPT", "checkpoint is invalid; restart the task")
	}
	return c, nil
}
func SaveCheckpoint(dir string, c Checkpoint) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	r, err := os.OpenRoot(dir)
	if err != nil {
		return model.Err("DISK_ERROR", "cannot open task directory")
	}
	defer r.Close()
	f, err := r.OpenFile("checkpoint.tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return model.Err("DISK_ERROR", "cannot write checkpoint")
	}
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = r.Rename("checkpoint.tmp", "checkpoint.json")
	}
	if err == nil {
		d, e := os.Open(dir)
		if e == nil {
			err = d.Sync()
			_ = d.Close()
		} else {
			err = e
		}
	}
	if err != nil {
		return model.Err("DISK_ERROR", "cannot commit checkpoint")
	}
	return nil
}
func OpenPart(dir string) (*os.File, error) {
	r, e := os.OpenRoot(dir)
	if e != nil {
		return nil, e
	}
	defer r.Close()
	return r.OpenFile("content.part", os.O_CREATE|os.O_RDWR, 0600)
}
func HashReader(r io.Reader) (string, error) {
	h := sha256.New()
	_, e := io.Copy(h, r)
	return hex.EncodeToString(h.Sum(nil)), e
}
func VerifyBlocks(ctx context.Context, f *os.File, c *Checkpoint) error {
	info, e := f.Stat()
	if e != nil {
		return e
	}
	for block, hash := range c.Blocks {
		length := BlockSize
		if c.Size != nil && *c.Size-block*BlockSize < length {
			length = *c.Size - block*BlockSize
		}
		if length <= 0 || block < 0 || block*BlockSize+length > info.Size() {
			delete(c.Blocks, block)
			continue
		}
		got, e := HashReader(contextReader{ctx, io.NewSectionReader(f, block*BlockSize, length)})
		if e != nil {
			return e
		}
		if got != hash {
			delete(c.Blocks, block)
		}
	}
	return nil
}
func CompleteFile(ctx context.Context, t model.Task, f *os.File, c *Checkpoint) (model.File, error) {
	if e := f.Sync(); e != nil {
		return model.File{}, model.Err("DISK_ERROR", "cannot sync download")
	}
	info, e := f.Stat()
	if e != nil {
		return model.File{}, e
	}
	hash, e := HashReader(contextReader{ctx, io.NewSectionReader(f, 0, info.Size())})
	if e != nil {
		return model.File{}, e
	}
	if t.SHA256 != "" && hash != t.SHA256 {
		return model.File{}, model.Err("CHECKSUM_MISMATCH", "download does not match expected SHA-256")
	}
	name := t.Metadata.Files[0].Name
	if t.Filename != "" {
		name = t.Filename
	}
	if !SafeOutputName(name) {
		return model.File{}, model.Err("INVALID_PATH", "invalid output filename")
	}
	r, e := os.OpenRoot(t.Dir)
	if e != nil {
		return model.File{}, e
	}
	defer r.Close()
	// Hard-link publication is atomic and never overwrites an existing file.
	base := name
	for suffix := 0; ; suffix++ {
		if suffix > 0 {
			name = fmt.Sprintf("%s.%d", base, suffix)
		}
		e = r.Link("content.part", name)
		if e == nil {
			break
		}
		if !os.IsExist(e) {
			return model.File{}, model.Err("DISK_ERROR", "cannot publish output")
		}
		existing, statErr := r.Stat(name)
		if statErr == nil && os.SameFile(info, existing) {
			break
		}
		if suffix >= 10000 {
			return model.File{}, model.Err("OUTPUT_EXISTS", "too many output files with the same name")
		}
	}
	c.Complete = true
	c.Hash = hash
	c.Output = name
	if e = SaveCheckpoint(t.Dir, *c); e != nil {
		_ = r.Remove(name)
		return model.File{}, e
	}
	_ = r.Remove("content.part")
	file := t.Metadata.Files[0]
	file.Name = name
	size := info.Size()
	file.Size = &size
	file.Completed = size
	file.Ready = true
	file.Path = filepath.Join(t.Dir, name)
	file.LocalSHA256 = hash
	file.VerifiedModTime = info.ModTime().UnixNano()
	file.Integrity = "transfer_complete"
	if t.SHA256 != "" {
		file.Integrity = "sha256_verified"
	}
	return file, nil
}
func RecoverComplete(ctx context.Context, t model.Task, c Checkpoint) (*model.File, error) {
	if !c.Complete {
		return nil, nil
	}
	name := t.Metadata.Files[0].Name
	if t.Filename != "" {
		name = t.Filename
	}
	if c.Output != "" {
		name = c.Output
	}
	if !SafeOutputName(name) {
		return nil, model.Err("CHECKPOINT_CORRUPT", "invalid checkpoint output name")
	}
	r, e := os.OpenRoot(t.Dir)
	if e != nil {
		return nil, e
	}
	defer r.Close()
	f, e := r.Open(name)
	if e != nil {
		return nil, model.Err("LOCAL_DATA_CHANGED", "completed file is missing; restart the task")
	}
	defer f.Close()
	hash, e := HashReader(contextReader{ctx, f})
	if e != nil {
		return nil, e
	}
	if hash != c.Hash || (t.SHA256 != "" && hash != t.SHA256) {
		return nil, model.Err("LOCAL_DATA_CHANGED", "completed file changed; restart the task")
	}
	info, e := f.Stat()
	if e != nil {
		return nil, e
	}
	file := t.Metadata.Files[0]
	file.Name = name
	size := info.Size()
	file.Size = &size
	file.Completed = size
	file.Ready = true
	file.Path = filepath.Join(t.Dir, name)
	file.LocalSHA256 = hash
	file.VerifiedModTime = info.ModTime().UnixNano()
	file.Integrity = "transfer_complete"
	if t.SHA256 != "" {
		file.Integrity = "sha256_verified"
	}
	return &file, nil
}

type LimitedReader struct {
	Context context.Context
	Reader  io.Reader
	Limit   *rate.Limiter
}
type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if e := r.ctx.Err(); e != nil {
		return 0, e
	}
	return r.reader.Read(p)
}

type countingReader struct {
	reader io.Reader
	add    func(int64)
}

func (r countingReader) Read(p []byte) (int, error) {
	n, e := r.reader.Read(p)
	if n > 0 {
		r.add(int64(n))
	}
	return n, e
}

type idleReader struct {
	reader io.Reader
	cancel context.CancelFunc
}

func (r idleReader) Read(p []byte) (int, error) {
	timer := time.AfterFunc(30*time.Second, r.cancel)
	n, e := r.reader.Read(p)
	timer.Stop()
	return n, e
}

func (r LimitedReader) Read(p []byte) (int, error) {
	chunk := 32 << 10
	// Keep limiter waits short enough to report progress even at very low rates.
	// Unlimited transfers retain the normal 32 KiB copy granularity.
	if limit := r.Limit.Limit(); limit != rate.Inf && limit < rate.Limit(4*chunk) {
		chunk = max(1, int(limit/4))
	}
	if len(p) > chunk {
		p = p[:chunk]
	}
	if e := r.Context.Err(); e != nil {
		return 0, e
	}
	n, e := r.Reader.Read(p)
	if n > 0 {
		if waitErr := r.Limit.WaitN(r.Context, n); waitErr != nil {
			return 0, waitErr
		}
	}
	return n, e
}

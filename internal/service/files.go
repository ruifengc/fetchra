package service

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/RyanChen-cn/fetchra/internal/engine"
	"github.com/RyanChen-cn/fetchra/internal/model"
)

// OpenFile exposes only a ready task file. The caller cannot supply a filesystem
// path. Hashing happens outside the control loop so large exports do not block
// pause, scheduling, or other clients.
func (s *Service) OpenFile(ctx context.Context, id string, index int) (*os.File, model.File, error) {
	type target struct {
		dir  string
		file model.File
	}
	value, err := s.do(ctx, func() (any, error) {
		t, err := s.find(model.Params{TaskID: id})
		if err != nil {
			return nil, err
		}
		if t.Metadata == nil || index < 0 || index >= len(t.Metadata.Files) {
			return nil, model.Err("INVALID_ARGUMENT", "invalid file index")
		}
		f := t.Metadata.Files[index]
		if !f.Ready || f.Path == "" {
			return nil, model.Err("FILE_NOT_READY", "file has not finished verification")
		}
		return target{t.Dir, f}, nil
	})
	if err != nil {
		return nil, model.File{}, err
	}
	result := value.(target)
	rel, err := filepath.Rel(result.dir, result.file.Path)
	if err != nil || !filepath.IsLocal(rel) {
		return nil, model.File{}, model.Err("INVALID_PATH", "invalid task output path")
	}
	downloadRoot, err := os.OpenRoot(s.root)
	if err != nil {
		return nil, model.File{}, model.Err("DISK_ERROR", "cannot open download root")
	}
	root, err := downloadRoot.OpenRoot(id)
	downloadRoot.Close()
	if err != nil {
		return nil, model.File{}, model.Err("LOCAL_DATA_CHANGED", "task directory is missing or outside download root")
	}
	f, err := root.Open(rel)
	root.Close()
	if err != nil {
		return nil, model.File{}, model.Err("LOCAL_DATA_CHANGED", "ready file is missing or outside task directory")
	}
	success := false
	defer func() {
		if !success {
			f.Close()
		}
	}()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || result.file.Size == nil || info.Size() != *result.file.Size {
		return nil, model.File{}, model.Err("LOCAL_DATA_CHANGED", "ready file size or type changed")
	}
	// Store hashes by inode/size/mtime. Subsequent HEAD and segment requests avoid
	// re-reading the complete file, but an observed file change invalidates it.
	key := filepath.Join(result.dir, rel)
	s.exportMu.Lock()
	cached, exists := s.exports[key]
	s.exportMu.Unlock()
	if result.file.LocalSHA256 != "" && result.file.VerifiedModTime != 0 && result.file.VerifiedModTime != info.ModTime().UnixNano() {
		return nil, model.File{}, model.Err("LOCAL_DATA_CHANGED", "ready file timestamp changed; resume the task to reverify it before retrieving")
	}
	hash := ""
	if result.file.LocalSHA256 != "" && result.file.VerifiedModTime == info.ModTime().UnixNano() {
		hash = result.file.LocalSHA256
	}
	if exists && os.SameFile(info, cached.info) && info.Size() == cached.info.Size() && info.ModTime() == cached.info.ModTime() {
		hash = cached.hash
	}
	if hash == "" {
		hash, err = engine.HashReader(exportReader{ctx, f})
		if err != nil {
			return nil, model.File{}, err
		}
		after, err := f.Stat()
		if err != nil || after.Size() != info.Size() || after.ModTime() != info.ModTime() {
			return nil, model.File{}, model.Err("LOCAL_DATA_CHANGED", "file changed while preparing export")
		}
		if result.file.LocalSHA256 != "" && result.file.LocalSHA256 != hash {
			return nil, model.File{}, model.Err("LOCAL_DATA_CHANGED", "ready file checksum changed")
		}
		s.exportMu.Lock()
		// Bound long-lived server memory; this is an optimization, not persisted state.
		if len(s.exports) >= 256 {
			clear(s.exports)
		}
		s.exports[key] = exportHash{info, hash}
		s.exportMu.Unlock()
	}
	if _, err = f.Seek(0, 0); err != nil {
		return nil, model.File{}, err
	}
	result.file.LocalSHA256 = hash
	success = true
	return f, result.file, nil
}

type exportHash struct {
	info os.FileInfo
	hash string
}
type exportReader struct {
	ctx context.Context
	f   *os.File
}

func (r exportReader) Read(b []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.f.Read(b)
}

// Exports and Torrent uploads share the node's upload budget.
func (s *Service) ExportReader(ctx context.Context, r io.ReadSeeker) io.ReadSeeker {
	return exportSeeker{Reader: engine.LimitedReader{Context: ctx, Reader: r, Limit: s.registry.UploadLimit}, Seeker: r}
}

type exportSeeker struct {
	io.Reader
	io.Seeker
}

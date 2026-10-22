package engine

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
)

// rootStorage confines every read/write to an os.Root, including when a path
// component is replaced by a symlink. Piece completion is deliberately volatile:
// persisted data must be hash checked before being trusted after restart.
type rootStorage struct{ dir string }
type diskFile struct {
	name           string
	offset, length int64
}
type diskTorrent struct {
	root     *os.Root
	files    []diskFile
	mu       sync.Mutex
	complete map[int]bool
	failure  error
}
type diskPiece struct {
	torrent *diskTorrent
	piece   metainfo.Piece
}

func torrentPaths(info *metainfo.Info) ([]diskFile, error) {
	if !SafeName(info.BestName()) {
		return nil, os.ErrInvalid
	}
	var files []diskFile
	var offset int64
	seen := map[string]bool{}
	for _, f := range info.UpvertedFiles() {
		parts := append([]string{info.BestName()}, f.BestPath()...)
		for _, part := range parts {
			if !SafeName(part) {
				return nil, os.ErrInvalid
			}
		}
		name := filepath.Join(parts...)
		if seen[name] || f.Length < 0 {
			return nil, os.ErrInvalid
		}
		seen[name] = true
		files = append(files, diskFile{name, offset, f.Length})
		offset += f.Length
		if offset < 0 {
			return nil, os.ErrInvalid
		}
	}
	return files, nil
}
func (s rootStorage) OpenTorrent(ctx context.Context, info *metainfo.Info, hash metainfo.Hash) (storage.TorrentImpl, error) {
	files, e := torrentPaths(info)
	if e != nil {
		return storage.TorrentImpl{}, e
	}
	if e = PrepareDir(s.dir); e != nil {
		return storage.TorrentImpl{}, e
	}
	r, e := os.OpenRoot(s.dir)
	if e != nil {
		return storage.TorrentImpl{}, e
	}
	t := &diskTorrent{root: r, files: files, complete: map[int]bool{}}
	for _, file := range files {
		if file.length == 0 {
			f, e := t.openWrite(file)
			if e != nil {
				r.Close()
				return storage.TorrentImpl{}, e
			}
			f.Close()
		}
	}
	return storage.TorrentImpl{Piece: func(p metainfo.Piece) storage.PieceImpl { return &diskPiece{t, p} }, Close: r.Close}, nil
}
func (t *diskTorrent) openWrite(file diskFile) (*os.File, error) {
	parts := strings.Split(filepath.Dir(file.name), string(filepath.Separator))
	dir := ""
	for _, part := range parts {
		dir = filepath.Join(dir, part)
		if e := t.root.Mkdir(dir, 0700); e != nil && !os.IsExist(e) {
			return nil, e
		}
	}
	f, e := t.root.OpenFile(file.name, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	return f, nil
}
func (p *diskPiece) ReadAt(b []byte, off int64) (int, error)  { return p.transfer(b, off, false) }
func (p *diskPiece) WriteAt(b []byte, off int64) (int, error) { return p.transfer(b, off, true) }
func (p *diskPiece) transfer(b []byte, off int64, write bool) (int, error) {
	if off < 0 || off >= p.piece.Length() {
		if len(b) == 0 {
			return 0, nil
		}
		return 0, io.EOF
	}
	if int64(len(b)) > p.piece.Length()-off {
		b = b[:p.piece.Length()-off]
	}
	abs := p.piece.Offset() + off
	written := 0
	for _, file := range p.torrent.files {
		if abs >= file.offset+file.length || abs < file.offset {
			continue
		}
		n := min(int64(len(b)-written), file.offset+file.length-abs)
		if n <= 0 {
			break
		}
		var f *os.File
		var e error
		if write {
			f, e = p.torrent.openWrite(file)
		} else {
			f, e = p.torrent.root.Open(file.name)
		}
		if e != nil {
			if write {
				p.fail(e)
			}
			return written, e
		}
		var done int
		if write {
			done, e = f.WriteAt(b[written:written+int(n)], abs-file.offset)
		} else {
			done, e = f.ReadAt(b[written:written+int(n)], abs-file.offset)
		}
		f.Close()
		written += done
		abs += int64(done)
		if e != nil {
			if write {
				p.fail(e)
			}
			return written, e
		}
		if written == len(b) {
			return written, nil
		}
	}
	return written, io.EOF
}
func (p *diskPiece) fail(e error) { p.torrent.mu.Lock(); p.torrent.failure = e; p.torrent.mu.Unlock() }
func (p *diskPiece) MarkComplete() error {
	for _, file := range p.torrent.files {
		if file.offset+file.length <= p.piece.Offset() || file.offset >= p.piece.Offset()+p.piece.Length() {
			continue
		}
		f, e := p.torrent.root.OpenFile(file.name, os.O_RDWR, 0600)
		if e != nil {
			p.fail(e)
			return e
		}
		e = f.Sync()
		f.Close()
		if e != nil {
			p.fail(e)
			return e
		}
	}
	p.torrent.mu.Lock()
	p.torrent.complete[p.piece.Index()] = true
	p.torrent.mu.Unlock()
	return nil
}
func (p *diskPiece) MarkNotComplete() error {
	p.torrent.mu.Lock()
	delete(p.torrent.complete, p.piece.Index())
	p.torrent.mu.Unlock()
	return nil
}
func (p *diskPiece) Completion() storage.Completion {
	p.torrent.mu.Lock()
	defer p.torrent.mu.Unlock()
	return storage.Completion{Ok: true, Complete: p.torrent.complete[p.piece.Index()], Err: p.torrent.failure}
}

type metadataStorage struct{}

func (metadataStorage) OpenTorrent(context.Context, *metainfo.Info, metainfo.Hash) (storage.TorrentImpl, error) {
	return storage.TorrentImpl{Piece: func(metainfo.Piece) storage.PieceImpl { return noPayload{} }, Close: func() error { return nil }}, nil
}

type noPayload struct{}

func (noPayload) ReadAt([]byte, int64) (int, error)  { return 0, io.EOF }
func (noPayload) WriteAt([]byte, int64) (int, error) { return 0, os.ErrPermission }
func (noPayload) MarkComplete() error                { return nil }
func (noPayload) MarkNotComplete() error             { return nil }
func (noPayload) Completion() storage.Completion     { return storage.Completion{Ok: true} }

package engine

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/RyanChen-cn/fetchra/internal/model"
	anaclog "github.com/anacrolix/log"
	torrentlib "github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"golang.org/x/time/rate"
)

type Torrent struct{ client *torrentlib.Client }

func NewTorrent(root string, dl, ul *rate.Limiter) (*Torrent, error) {
	cfg := torrentlib.NewDefaultClientConfig()
	cfg.DataDir = root
	cfg.DownloadRateLimiter = dl
	cfg.UploadRateLimiter = ul
	cfg.NoDefaultPortForwarding = true
	cfg.DisableWebtorrent = true
	cfg.EstablishedConnsPerTorrent = 50
	cfg.ListenPort = 0
	cfg.Seed = true // Per-task upload flags and handle lifetime enforce the policy.
	cfg.DefaultStorage = metadataStorage{}
	cfg.Logger = anaclog.NewLogger("fetchra")
	cfg.Logger.Handlers = nil
	cfg.Slogger = slog.New(slog.NewTextHandler(io.Discard, nil))
	c, err := torrentlib.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return &Torrent{c}, nil
}
func (t *Torrent) Close() error { return errors.Join(t.client.Close()...) }
func TorrentIdentity(s model.Source) (string, error) {
	spec, e := torrentSpec(s, nil)
	if e != nil {
		return "", e
	}
	return hex.EncodeToString(spec.InfoHash[:]), nil
}
func torrentSpec(s model.Source, data []byte) (*torrentlib.TorrentSpec, error) {
	if len(data) == 0 {
		data = s.TorrentData
	}
	var spec *torrentlib.TorrentSpec
	var e error
	if len(data) > 0 {
		mi, e := metainfo.Load(bytes.NewReader(data))
		if e != nil {
			return nil, model.Err("INVALID_TORRENT", "invalid stored torrent")
		}
		spec, e = torrentlib.TorrentSpecFromMetaInfoErr(mi)
		if e != nil {
			return nil, model.Err("INVALID_TORRENT", "invalid torrent info")
		}
	} else if s.URL != "" {
		spec, e = torrentlib.TorrentSpecFromMagnetUri(s.URL)
		if e != nil {
			return nil, model.Err("INVALID_TORRENT", "invalid magnet URI")
		}
	} else {
		f, e := os.Open(s.TorrentPath)
		if e != nil {
			return nil, model.Err("INVALID_TORRENT", "cannot read torrent file")
		}
		defer f.Close()
		mi, e := metainfo.Load(io.LimitReader(f, model.MaxTorrentSize+1))
		if e != nil {
			return nil, model.Err("INVALID_TORRENT", "invalid torrent file")
		}
		spec, e = torrentlib.TorrentSpecFromMetaInfoErr(mi)
		if e != nil {
			return nil, model.Err("INVALID_TORRENT", "invalid torrent info")
		}
	}
	if spec.InfoHash == (metainfo.Hash{}) {
		return nil, model.Err("UNSUPPORTED_PROTOCOL", "first version requires a BitTorrent v1 info hash")
	}
	if len(data) > 0 && s.URL != "" {
		original, e := torrentlib.TorrentSpecFromMagnetUri(s.URL)
		if e != nil {
			return nil, model.Err("INVALID_TORRENT", "invalid original magnet")
		}
		spec.PeerAddrs = original.PeerAddrs
	}
	spec.DisallowDataDownload = true
	spec.DisallowDataUpload = true
	spec.DisableInitialPieceCheck = true
	return spec, nil
}
func torrentMetadata(mi *metainfo.MetaInfo) (*model.Metadata, error) {
	info, e := mi.UnmarshalInfo()
	if e != nil || !info.HasV1() || info.HasV2() {
		return nil, model.Err("UNSUPPORTED_PROTOCOL", "first version supports v1 torrent metainfo only")
	}
	paths, e := torrentPaths(&info)
	if e != nil {
		return nil, model.Err("INVALID_PATH", "torrent contains invalid or duplicate file paths")
	}
	files := make([]model.File, len(paths))
	var size int64
	for i, f := range paths {
		n := f.length
		files[i] = model.File{Index: i, Name: filepath.ToSlash(f.name), Size: &n, Priority: "normal"}
		size += n
	}
	var data bytes.Buffer
	if e = mi.Write(&data); e != nil {
		return nil, e
	}
	hash := mi.HashInfoBytes()
	return &model.Metadata{InfoHash: hex.EncodeToString(hash[:]), Files: files, Size: &size, TorrentData: data.Bytes(), Capabilities: model.Capabilities{Resume: true, ResumeConfidence: "piece_hashes", Parallel: true, FileSelection: true}}, nil
}
func (t *Torrent) Probe(ctx context.Context, s model.Source) (*model.Metadata, error) {
	spec, e := torrentSpec(s, nil)
	if e != nil {
		return nil, e
	}
	if len(spec.InfoBytes) > 0 {
		var reader io.Reader
		if len(s.TorrentData) > 0 {
			reader = bytes.NewReader(s.TorrentData)
		} else {
			f, err := os.Open(s.TorrentPath)
			if err != nil {
				return nil, model.Err("INVALID_TORRENT", "cannot read torrent file")
			}
			defer f.Close()
			reader = io.LimitReader(f, model.MaxTorrentSize+1)
		}
		mi, err := metainfo.Load(reader)
		if err != nil {
			return nil, model.Err("INVALID_TORRENT", "invalid torrent file")
		}
		return torrentMetadata(mi)
	}
	spec.Storage = metadataStorage{}
	handle, _, e := t.client.AddTorrentSpec(spec)
	if e != nil {
		return nil, model.Err("ENGINE_ERROR", "cannot add magnet")
	}
	defer handle.Drop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-handle.GotInfo():
		return torrentMetadata(pointerMeta(handle.Metainfo()))
	}
}
func pointerMeta(mi metainfo.MetaInfo) *metainfo.MetaInfo { return &mi }
func (t *Torrent) Download(ctx context.Context, task model.Task, report Report) error {
	if e := PrepareDir(task.Dir); e != nil {
		return e
	}
	spec, e := torrentSpec(task.Source, task.Metadata.TorrentData)
	if e != nil {
		return e
	}
	spec.Storage = rootStorage{task.Dir}
	handle, _, e := t.client.AddTorrentSpec(spec)
	if e != nil {
		return model.Err("ENGINE_ERROR", "cannot restore torrent")
	}
	defer handle.Drop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-handle.GotInfo():
	}
	files := handle.Files()
	hashes := map[int]string{}
	stamps := map[int]int64{}
	report(Progress{Phase: "checking", Files: task.Metadata.Files})
	pieces := map[int]bool{}
	for _, f := range task.Metadata.Files {
		if !f.Selected {
			continue
		}
		for i := files[f.Index].BeginPieceIndex(); i < files[f.Index].EndPieceIndex(); i++ {
			pieces[i] = true
		}
	}
	for i := range pieces {
		if e := handle.Piece(i).VerifyDataContext(ctx); e != nil {
			return ctxOr(ctx, "DISK_ERROR", "cannot verify torrent data")
		}
	}
	for _, f := range task.Metadata.Files {
		if f.Selected {
			prio := torrentlib.PiecePriorityNormal
			if f.Priority == "high" {
				prio = torrentlib.PiecePriorityHigh
			}
			files[f.Index].SetPriority(prio)
		}
	}
	handle.AllowDataUpload()
	handle.AllowDataDownload()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		out := append([]model.File(nil), task.Metadata.Files...)
		all := true
		for i, f := range out {
			if !f.Selected {
				continue
			}
			ready := true
			for p := files[i].BeginPieceIndex(); p < files[i].EndPieceIndex(); p++ {
				state := handle.Piece(p).State()
				if state.Err != nil {
					return model.Err("DISK_ERROR", "torrent storage failed")
				}
				if !state.Complete || state.Checking || state.Hashing || state.Marking {
					ready = false
					break
				}
			}
			out[i].Completed = files[i].BytesCompleted()
			out[i].Ready = ready
			if ready {
				out[i].Path = filepath.Join(task.Dir, filepath.FromSlash(f.Name))
				out[i].Integrity = "torrent_pieces_verified"
				if hashes[i] == "" {
					root, err := os.OpenRoot(task.Dir)
					if err != nil {
						return model.Err("DISK_ERROR", "cannot open ready torrent file")
					}
					local, err := root.Open(filepath.FromSlash(f.Name))
					root.Close()
					if err != nil {
						return model.Err("DISK_ERROR", "cannot open ready torrent file")
					}
					hash, err := HashReader(contextReader{ctx, local})
					info, statErr := local.Stat()
					local.Close()
					if statErr != nil {
						return model.Err("DISK_ERROR", "cannot stat ready torrent file")
					}
					stamps[i] = info.ModTime().UnixNano()
					if err != nil {
						return err
					}
					hashes[i] = hash
				}
				out[i].LocalSHA256 = hashes[i]
				out[i].VerifiedModTime = stamps[i]
			} else {
				all = false
			}
		}
		stats := handle.Stats()
		if all {
			report(Progress{Files: out, Seeding: task.Policy.Seed, Transferred: stats.BytesReadData.Int64()})
			if !task.Policy.Seed {
				return nil
			}
		} else {
			report(Progress{Phase: "downloading", Files: out, Transferred: stats.BytesReadData.Int64()})
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
func ctxOr(ctx context.Context, code, msg string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return model.Err(code, msg)
}

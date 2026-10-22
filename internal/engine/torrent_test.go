package engine

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/RyanChen-cn/fetchra/internal/model"
	torrentlib "github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
)

func localTorrentClient(t *testing.T, dir string) *torrentlib.Client {
	t.Helper()
	cfg := torrentlib.NewDefaultClientConfig()
	cfg.DataDir = dir
	cfg.ListenHost = torrentlib.LoopbackListenHost
	cfg.ListenPort = 0
	cfg.NoDHT = true
	cfg.DisableTrackers = true
	cfg.NoDefaultPortForwarding = true
	cfg.DisableWebtorrent = true
	cfg.DisableUTP = true
	cfg.DisableIPv6 = true
	cfg.Seed = true
	client, e := torrentlib.NewClient(cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { client.Close() })
	return client
}
func TestTorrentMagnetSelectionRecovery(t *testing.T) {
	seedRoot := t.TempDir()
	bundle := filepath.Join(seedRoot, "bundle")
	if e := os.Mkdir(bundle, 0700); e != nil {
		t.Fatal(e)
	}
	data := map[string][]byte{"a.txt": bytes.Repeat([]byte("a"), 10000), "b.txt": bytes.Repeat([]byte("b"), 20000), "c.txt": bytes.Repeat([]byte("c"), 60000), "empty": {}}
	for name, b := range data {
		if e := os.WriteFile(filepath.Join(bundle, name), b, 0600); e != nil {
			t.Fatal(e)
		}
	}
	info := metainfo.Info{PieceLength: 16 << 10}
	if e := info.BuildFromFilePath(bundle); e != nil {
		t.Fatal(e)
	}
	encoded, e := bencode.Marshal(info)
	if e != nil {
		t.Fatal(e)
	}
	mi := metainfo.MetaInfo{InfoBytes: encoded}
	seed := localTorrentClient(t, seedRoot)
	spec := torrentlib.TorrentSpecFromMetaInfo(&mi)
	spec.Storage = storage.NewFile(seedRoot)
	handle, _, e := seed.AddTorrentSpec(spec)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if e = handle.VerifyDataContext(ctx); e != nil {
		t.Fatal(e)
	}
	for !handle.Complete().Bool() {
		select {
		case <-ctx.Done():
			for i := 0; i < handle.NumPieces(); i++ {
				t.Logf("piece %d: %+v", i, handle.Piece(i).State())
			}
			t.Fatal("fixture seeder is incomplete")
		case <-time.After(10 * time.Millisecond):
		}
	}
	client := localTorrentClient(t, t.TempDir())
	download := &Torrent{client}
	hash := mi.HashInfoBytes()
	source := model.Source{Protocol: "torrent", URL: fmt.Sprintf("magnet:?xt=urn:btih:%x&x.pe=%s", hash[:], url.QueryEscape(seed.ListenAddrs()[0].String()))}
	m, e := download.Probe(ctx, source)
	if e != nil {
		t.Fatal(e)
	}
	if len(m.Files) != 4 {
		t.Fatalf("wrong metadata: %+v", m.Files)
	}
	dir := t.TempDir()
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatal("probe wrote payload")
	}
	m.Files[0].Selected = true
	task := model.Task{Source: source, Metadata: m, Dir: dir, Policy: model.TaskPolicy{Priority: "normal", Connections: 4}}
	var result []model.File
	if e = download.Download(ctx, task, func(p Progress) { result = p.Files }); e != nil {
		t.Fatal(e)
	}
	if !result[0].Ready || result[1].Ready {
		t.Fatal("wrong selection readiness")
	}
	got, e := os.ReadFile(result[0].Path)
	if e != nil || !bytes.Equal(got, data["a.txt"]) {
		t.Fatal("selected file mismatch", e)
	}
	// A full seed is not necessary for the selected-file task to complete.
	if _, e = os.Stat(filepath.Join(dir, "bundle", "c.txt")); !os.IsNotExist(e) {
		t.Fatal("downloaded a distant unselected file")
	}
	for i := range m.Files {
		m.Files[i].Selected = i == 1 || i == 3
	}
	if e = download.Download(ctx, task, func(p Progress) { result = p.Files }); e != nil {
		t.Fatal(e)
	}
	if !result[1].Ready || !result[3].Ready {
		t.Fatal("next selection not delivered")
	}
	got, e = os.ReadFile(result[1].Path)
	if e != nil || !bytes.Equal(got, data["b.txt"]) {
		t.Fatal("resume mismatch", e)
	}
	// Corrupt a previously complete piece; recovery must reacquire it.
	f, e := os.OpenFile(result[1].Path, os.O_RDWR, 0600)
	if e != nil {
		t.Fatal(e)
	}
	_, _ = f.WriteAt([]byte("corrupt"), 0)
	f.Close()
	if e = download.Download(ctx, task, func(Progress) {}); e != nil {
		t.Fatal(e)
	}
	got, _ = os.ReadFile(result[1].Path)
	if !bytes.Equal(got, data["b.txt"]) {
		t.Fatal("corrupt torrent data was not repaired")
	}
}
func TestTorrentRejectsPaths(t *testing.T) {
	info := metainfo.Info{Name: "bundle", PieceLength: 16 << 10, Files: []metainfo.FileInfo{{Path: []string{"..", "outside"}, Length: 1}}}
	if _, e := torrentPaths(&info); e == nil {
		t.Fatal("accepted traversal")
	}
}

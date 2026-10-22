package model

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPublicTaskBoundedAndPrivate(t *testing.T) {
	task := Task{Source: Source{Protocol: "torrent", URL: "magnet:?xt=urn:btih:abc&tr=https%3A%2F%2Ftracker.invalid%2F%3Ftoken%3Dsecret"}, Metadata: &Metadata{FinalURL: "https://example.invalid/?secret", TorrentData: []byte("private metadata")}}
	for i := 0; i < 1000; i++ {
		task.Metadata.Files = append(task.Metadata.Files, File{Index: i, Selected: i < 500, Ready: i < 10, VerifiedModTime: 12345})
	}
	public := task.Public()
	if len(public.Metadata.Files) != 10 || public.Metadata.FileCount != 1000 || public.Metadata.SelectedCount != 500 || public.Metadata.ReadyCount != 10 {
		t.Fatal("unbounded or incorrect preview")
	}
	b, _ := json.Marshal(public)
	if strings.Contains(string(b), "verified_mod_time_ns") {
		t.Fatal("internal file verification stamp exposed")
	}
	if strings.Contains(string(b), "secret") || strings.Contains(string(b), "private metadata") {
		t.Fatal("private source data exposed")
	}
	if len(task.Metadata.Files) != 1000 || len(task.Metadata.TorrentData) == 0 {
		t.Fatal("public snapshot mutated persisted metadata")
	}
}

func TestSnapshotIsolation(t *testing.T) {
	n := int64(7)
	eta := float64(3)
	original := Task{Dir: "directory", Source: Source{TorrentData: []byte{1}}, Metadata: &Metadata{TorrentData: []byte{2}, Size: &n, Files: []File{{Name: "original", Size: &n}}}, ETA: &eta, Issue: Err("OLD", "old")}
	copy := original.Copy()
	copy.Metadata.Files[0].Name = "changed"
	*copy.Metadata.Files[0].Size = 10
	*copy.Metadata.Size = 11
	copy.Source.TorrentData[0] = 9
	copy.Metadata.TorrentData[0] = 9
	*copy.ETA = 20
	copy.Issue.Message = "changed"
	if original.Metadata.Files[0].Name != "original" || n != 7 || original.Source.TorrentData[0] != 1 || original.Metadata.TorrentData[0] != 2 || eta != 3 || original.Issue.Message != "old" || copy.Dir != original.Dir {
		t.Fatal("snapshot shares mutable state")
	}
}

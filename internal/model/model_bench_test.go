package model

import "testing"

func BenchmarkTaskCopy(b *testing.B) {
	size := int64(1024)
	task := Task{ID: "task", Metadata: &Metadata{TorrentData: make([]byte, 512<<10)}}
	for i := 0; i < 1000; i++ {
		task.Metadata.Files = append(task.Metadata.Files, File{Index: i, Name: "bundle/file", Size: &size, Selected: true})
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		copy := task.Copy()
		if len(copy.Metadata.Files) != 1000 {
			b.Fatal("incomplete snapshot")
		}
	}
}

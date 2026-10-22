package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/RyanChen-cn/fetchra/internal/engine"
	"github.com/RyanChen-cn/fetchra/internal/model"
	"github.com/RyanChen-cn/fetchra/internal/service"
)

func TestRemoteFetchCancellationAndResume(t *testing.T) {
	data := bytes.Repeat([]byte("z"), 3*int(engine.BlockSize)+123)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"stable"`)
		http.ServeContent(w, r, "large.bin", time.Time{}, bytes.NewReader(data))
	}))
	defer origin.Close()
	root := t.TempDir()
	s, err := service.New(filepath.Join(root, "state"), filepath.Join(root, "downloads"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ready := make(chan struct{})
	var mu sync.Mutex
	requests := map[int64]int{}
	handler := NodeHandler(s)
	node := httptest.NewServer(AuthenticatedHandler(testToken, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var start, end int64
		if r.Method == "GET" && len(r.URL.Path) > 7 && r.URL.Path[:7] == "/files/" && r.Header.Get("Range") != "" {
			if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err == nil && end > start {
				mu.Lock()
				requests[start]++
				mu.Unlock()
				if start >= engine.BlockSize {
					select {
					case <-ready:
					case <-r.Context().Done():
						return
					}
				}
			}
		}
		handler.ServeHTTP(w, r)
	})))
	defer node.Close()
	client, err := NewRemoteClient(RemoteClientConfig{Endpoint: node.URL, Token: testToken})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	task := decodeTask(t, rpcCall(t, client, "task_add", model.Add{Source: origin.URL + "/large.bin", Start: true}))
	result := rpcCall(t, client, "task_wait", model.Params{TaskID: task.ID, TimeoutSeconds: 10}).(map[string]any)
	if result["matched"] != true {
		t.Fatal("remote task did not complete")
	}
	fetchRoot := filepath.Join(root, "fetched")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := client.Fetch(ctx, task.ID, 0, fetchRoot); done <- err }()
	deadline := time.Now().Add(10 * time.Second)
	checkpointed := false
	for time.Now().Before(deadline) {
		paths, _ := filepath.Glob(filepath.Join(fetchRoot, "*", "checkpoint.json"))
		for _, path := range paths {
			raw, err := os.ReadFile(path)
			var cp engine.Checkpoint
			if err == nil && json.Unmarshal(raw, &cp) == nil && len(cp.Blocks) > 0 {
				checkpointed = true
			}
		}
		if checkpointed {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled fetch succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fetch ignored cancellation")
	}
	if !checkpointed {
		close(ready)
		t.Fatal("no durable local checkpoint")
	}
	close(ready)
	got, err := client.Fetch(context.Background(), task.ID, 0, fetchRoot)
	if err != nil {
		t.Fatal(err)
	}
	output, err := os.ReadFile(got.Path)
	if err != nil || !bytes.Equal(output, data) {
		t.Fatal("resumed fetch is corrupt", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if requests[0] != 1 {
		t.Fatal("re-downloaded verified first block", requests)
	}
}

package service

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
)

func invoke(t *testing.T, s *Service, method string, p any) any {
	t.Helper()
	b, _ := json.Marshal(p)
	v, e := s.Call(context.Background(), method, b)
	if e != nil {
		t.Fatalf("%s: %v", method, e)
	}
	return v
}
func waitState(t *testing.T, s *Service, id, state string) model.Task {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		v := invoke(t, s, "task_get", model.Params{TaskID: id}).(model.Task)
		if v.State == state {
			return v
		}
		if v.State == "failed" {
			t.Fatalf("task failed: %+v", v.Issue)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out waiting for", state)
	return model.Task{}
}
func newService(t *testing.T) (*Service, string, string) {
	t.Helper()
	root := t.TempDir()
	state, downloads := filepath.Join(root, "state"), filepath.Join(root, "downloads")
	s, e := New(state, downloads, nil)
	if e != nil {
		t.Fatal(e)
	}
	return s, state, downloads
}
func TestWorkflowIdempotencyAndPersistence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "file.txt", time.Time{}, bytes.NewReader([]byte("workflow data")))
	}))
	defer server.Close()
	s, state, downloads := newService(t)
	a := model.Add{Source: server.URL + "/file.txt?token=private", RequestKey: "stable"}
	added := invoke(t, s, "task_add", a).(model.Task)
	if added.Source.URL != server.URL+"/file.txt" {
		t.Fatal("signed query exposed")
	}
	same := invoke(t, s, "task_add", a).(model.Task)
	if same.ID != added.ID {
		t.Fatal("add was not idempotent")
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, _ := json.Marshal(a)
			v, e := s.Call(context.Background(), "task_add", b)
			if e != nil || v.(model.Task).ID != added.ID {
				t.Error("concurrent idempotency failed", e)
			}
		}()
	}
	wg.Wait()
	a.Source = server.URL + "/other"
	b, _ := json.Marshal(a)
	if _, e := s.Call(context.Background(), "task_add", b); e == nil || e.(*model.Error).Code != "REQUEST_CONFLICT" {
		t.Fatal("missing request conflict", e)
	}
	ready := waitState(t, s, added.ID, "ready")
	if _, e := os.Stat(filepath.Join(downloads, added.ID, "content.part")); !os.IsNotExist(e) {
		t.Fatal("probe wrote download data")
	}
	wrong := ready.Revision + 100
	b, _ = json.Marshal(model.Params{TaskID: added.ID, ExpectedRevision: &wrong})
	if _, e := s.Call(context.Background(), "task_start", b); e == nil || e.(*model.Error).Code != "REVISION_CONFLICT" {
		t.Fatal("missing revision conflict", e)
	}
	invoke(t, s, "task_start", model.Params{TaskID: added.ID})
	completed := waitState(t, s, added.ID, "completed")
	file := completed.Metadata.Files[0]
	if !file.Ready || file.Integrity != "transfer_complete" {
		t.Fatal("bad completion", file)
	}
	got, e := os.ReadFile(file.Path)
	if e != nil || string(got) != "workflow data" {
		t.Fatal("wrong file", e)
	}
	s.Close()
	s, e = New(state, downloads, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	restored := waitState(t, s, added.ID, "completed")
	if !restored.Metadata.Files[0].Ready {
		t.Fatal("restored output not checked")
	}
	events := invoke(t, s, "events_list", model.Params{}).(map[string]any)
	if len(events["events"].([]model.Event)) == 0 {
		t.Fatal("missing persisted events")
	}
	invoke(t, s, "task_wait", model.Params{TaskID: added.ID, Condition: "idle", TimeoutSeconds: 5})
	invoke(t, s, "task_remove", model.Params{TaskID: added.ID})
	if _, e = os.Stat(file.Path); e != nil {
		t.Fatal("remove deleted output", e)
	}
}

type fakeEngine struct {
	mu              sync.Mutex
	started         []string
	active, maximum int
}

func (f *fakeEngine) Close() error { return nil }
func (f *fakeEngine) Probe(context.Context, model.Source) (*model.Metadata, error) {
	n := int64(1)
	return &model.Metadata{Files: []model.File{{Index: 0, Name: "fake", Selected: true, Size: &n}}, Size: &n}, nil
}
func (f *fakeEngine) Download(ctx context.Context, t model.Task, r engine.Report) error {
	f.mu.Lock()
	f.started = append(f.started, t.ID)
	f.active++
	f.maximum = max(f.maximum, f.active)
	f.mu.Unlock()
	defer func() { f.mu.Lock(); f.active--; f.mu.Unlock() }()
	r(engine.Progress{Phase: "downloading", Files: t.Metadata.Files})
	<-ctx.Done()
	return ctx.Err()
}
func TestQueuePriorityAndPauseRelease(t *testing.T) {
	s, _, _ := newService(t)
	defer s.Close()
	fake := &fakeEngine{}
	_, e := s.do(context.Background(), func() (any, error) { s.registry.Engines["http"] = fake; return nil, nil })
	if e != nil {
		t.Fatal(e)
	}
	invoke(t, s, "policy_set", model.Params{ResourcePolicy: &model.Policy{Active: 1, Probes: 2, PerHost: 4}})
	ids := make([]string, 3)
	for i := range ids {
		v := invoke(t, s, "task_add", model.Add{Source: fmt.Sprintf("https://example.invalid/%d", i)}).(model.Task)
		ids[i] = v.ID
		waitState(t, s, v.ID, "ready")
	}
	invoke(t, s, "task_start", model.Params{TaskID: ids[0]})
	waitState(t, s, ids[0], "downloading")
	invoke(t, s, "task_start", model.Params{TaskID: ids[1]})
	invoke(t, s, "task_policy_set", model.Params{TaskID: ids[2], TaskPolicy: &model.TaskPolicy{Priority: "high", Connections: 4}})
	invoke(t, s, "task_start", model.Params{TaskID: ids[2]})
	invoke(t, s, "task_pause", model.Params{TaskID: ids[0]})
	invoke(t, s, "task_wait", model.Params{TaskID: ids[0], Condition: "idle", TimeoutSeconds: 5})
	waitState(t, s, ids[2], "downloading")
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.started) != 2 || fake.started[1] != ids[2] || fake.maximum != 1 {
		t.Fatalf("wrong queue or concurrency: %+v max=%d", fake.started, fake.maximum)
	}
}
func TestWaitTimeoutAndCancellationDoNotPause(t *testing.T) {
	s, _, _ := newService(t)
	defer s.Close()
	fake := &fakeEngine{}
	_, _ = s.do(context.Background(), func() (any, error) { s.registry.Engines["http"] = fake; return nil, nil })
	v := invoke(t, s, "task_add", model.Add{Source: "https://example.invalid/wait", Start: true}).(model.Task)
	waitState(t, s, v.ID, "downloading")
	result := invoke(t, s, "task_wait", model.Params{TaskID: v.ID, TimeoutSeconds: 1}).(map[string]any)
	if !result["timed_out"].(bool) {
		t.Fatal("wait was not bounded")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b, _ := json.Marshal(model.Params{TaskID: v.ID})
	if _, e := s.Call(ctx, "task_wait", b); e == nil {
		t.Fatal("cancel did not stop wait")
	}
	current := invoke(t, s, "task_get", model.Params{TaskID: v.ID}).(model.Task)
	if current.Desired != "running" {
		t.Fatal("wait cancellation paused download")
	}
}
func TestResumeReadyTask(t *testing.T) {
	s, _, _ := newService(t)
	defer s.Close()
	fake := &fakeEngine{}
	_, _ = s.do(context.Background(), func() (any, error) { s.registry.Engines["http"] = fake; return nil, nil })
	v := invoke(t, s, "task_add", model.Add{Source: "https://example.invalid/inspect"}).(model.Task)
	waitState(t, s, v.ID, "ready")
	invoke(t, s, "task_pause", model.Params{TaskID: v.ID})
	invoke(t, s, "task_resume", model.Params{TaskID: v.ID})
	waitState(t, s, v.ID, "downloading")
}

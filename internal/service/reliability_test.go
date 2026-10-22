package service

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/RyanChen-cn/fetchra/internal/model"
	"io"
	"path/filepath"
	"testing"
	"time"
)

func TestPersistenceFailureDoesNotApplyIntent(t *testing.T) {
	s, _, _ := newService(t)
	defer s.Close()
	fake := &fakeEngine{}
	_, err := s.do(context.Background(), func() (any, error) { s.registry.Engines["http"] = fake; return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	added := invoke(t, s, "task_add", model.Add{Source: "https://example.invalid/ongoing", Start: true}).(model.Task)
	before := waitState(t, s, added.ID, "downloading")
	_, err = s.do(context.Background(), func() (any, error) { return nil, s.store.Close() })
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(model.Params{TaskID: added.ID})
	if _, err = s.Call(context.Background(), "task_pause", raw); model.AsError(err).Code != "DISK_ERROR" {
		t.Fatal("pause persistence failure was not reported", err)
	}
	after := invoke(t, s, "task_get", model.Params{TaskID: added.ID}).(model.Task)
	if after.Desired != before.Desired || after.Revision != before.Revision || after.State == "paused" {
		t.Fatal("uncommitted pause changed task intent", before, after)
	}
	_, err = s.do(context.Background(), func() (any, error) {
		if s.jobs[added.ID].stopping {
			t.Error("uncommitted pause cancelled worker")
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
func TestProgressPersistenceFailureRevokesReady(t *testing.T) {
	s, _, _ := newService(t)
	defer s.Close()
	fake := &fakeEngine{}
	_, _ = s.do(context.Background(), func() (any, error) { s.registry.Engines["http"] = fake; return nil, nil })
	added := invoke(t, s, "task_add", model.Add{Source: "https://example.invalid/fail", Start: true}).(model.Task)
	waitState(t, s, added.ID, "downloading")
	_, err := s.do(context.Background(), func() (any, error) {
		s.store.Close()
		task := s.tasks[added.ID]
		task.State = "completed"
		task.Metadata.Files[0].Ready = true
		task.Metadata.Files[0].Path = filepath.Join(task.Dir, "file")
		s.record(task, "completed")
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	current := invoke(t, s, "task_get", model.Params{TaskID: added.ID}).(model.Task)
	if current.State != "failed" || current.Issue.Code != "DISK_ERROR" || current.Metadata.Files[0].Ready {
		t.Fatal("unpersisted completion was exposed", current)
	}
}
func TestPaginationLargeOffset(t *testing.T) {
	s, _, _ := newService(t)
	defer s.Close()
	invoke(t, s, "task_list", model.Params{Offset: int(^uint(0) >> 1), Limit: 200})
}

func TestExportUsesSharedUploadBudget(t *testing.T) {
	s, _, _ := newService(t)
	defer s.Close()
	invoke(t, s, "policy_set", model.Params{ResourcePolicy: &model.Policy{Active: 3, Probes: 2, PerHost: 4, UploadRate: 32 << 10}})
	// Consume the initial burst as if a Torrent upload had already used it.
	if !s.registry.UploadLimit.AllowN(time.Now(), s.registry.UploadLimit.Burst()) {
		t.Fatal("cannot consume initial burst")
	}
	payload := bytes.Repeat([]byte("x"), 16<<10)
	reader := s.ExportReader(context.Background(), bytes.NewReader(payload))
	before := time.Now()
	got, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatal("limited export corrupted data", err)
	}
	if time.Since(before) < 400*time.Millisecond {
		t.Fatal("export bypassed shared upload budget")
	}
}

package store

import (
	"github.com/RyanChen-cn/fetchra/internal/model"
	"testing"
)

func TestEventRetentionAndPolicy(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	task := model.Task{ID: "one", State: "ready"}
	for i := 0; i < 10002; i++ {
		if e = s.Save(task, "changed", "", nil); e != nil {
			t.Fatal(e)
		}
	}
	if _, _, e = s.Events(1, "", 50); e == nil || e.(*model.Error).Code != "CURSOR_EXPIRED" {
		t.Fatal("missing expired cursor", e)
	}
	events, cursor, e := s.Events(0, "", 2)
	if e != nil || len(events) != 2 || events[0].Cursor != 3 || cursor != 4 {
		t.Fatalf("bad event page: %+v %d %v", events, cursor, e)
	}
	p := model.DefaultPolicy()
	p.Active = 5
	if e = s.Policy(p); e != nil {
		t.Fatal(e)
	}
	tasks, got, e := s.Load()
	if e != nil || len(tasks) != 1 || got.Active != 5 {
		t.Fatal("persistence mismatch", e)
	}
}

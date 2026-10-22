package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/RyanChen-cn/fetchra/internal/engine"
	"github.com/RyanChen-cn/fetchra/internal/model"
	"github.com/RyanChen-cn/fetchra/internal/store"
)

type call struct {
	ctx   context.Context
	fn    func() (any, error)
	reply chan reply
}
type reply struct {
	value any
	err   error
}
type job struct {
	cancel     context.CancelFunc
	probe      bool
	stopping   bool
	lastBytes  int64
	lastAt     time.Time
	progressAt time.Time
}
type update struct {
	id       string
	job      *job
	metadata *model.Metadata
	progress *engine.Progress
	done     bool
	err      error
}
type Service struct {
	exportMu    sync.Mutex
	exports     map[string]exportHash
	ctx         context.Context
	cancel      context.CancelFunc
	closed      chan struct{}
	calls       chan call
	updates     chan update
	store       *store.Store
	registry    *engine.Registry
	tasks       map[string]*model.Task
	identities  map[string]string
	policy      model.Policy
	jobs        map[string]*job
	root        string
	credentials map[string]model.Credential
	workers     sync.WaitGroup
}

func New(stateDir, downloadDir string, credentials map[string]model.Credential) (*Service, error) {
	stateDir, e := filepath.Abs(stateDir)
	if e != nil {
		return nil, e
	}
	downloadDir, e = filepath.Abs(downloadDir)
	if e != nil {
		return nil, e
	}
	if e = engine.PrepareDir(downloadDir); e != nil {
		return nil, e
	}
	st, e := store.Open(stateDir)
	if e != nil {
		return nil, e
	}
	tasks, p, e := st.Load()
	if e != nil {
		st.Close()
		return nil, e
	}
	reg, e := engine.NewRegistry(downloadDir, credentials)
	if e != nil {
		st.Close()
		return nil, e
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{ctx: ctx, cancel: cancel, closed: make(chan struct{}), calls: make(chan call), updates: make(chan update, 256), store: st, registry: reg, tasks: map[string]*model.Task{}, identities: map[string]string{}, policy: p, jobs: map[string]*job{}, root: downloadDir, credentials: credentials, exports: map[string]exportHash{}}
	reg.SetPolicy(p)
	for i := range tasks {
		t := tasks[i]
		t.Dir = filepath.Join(downloadDir, t.ID)
		if t.Metadata != nil && t.Metadata.InfoHash != "" {
			s.identities[t.Metadata.InfoHash] = t.ID
		}
		if t.Source.Protocol == "torrent" {
			if id, e := engine.TorrentIdentity(t.Source); e == nil {
				s.identities[id] = t.ID
			}
		}
		if t.State == "completed" || t.Desired == "running" {
			t.State = "queued"
			t.Phase = "recovering"
			if t.Metadata != nil {
				for i := range t.Metadata.Files {
					t.Metadata.Files[i].Ready = false
					t.Metadata.Files[i].Path = ""
				}
			} else {
				t.State = "probing"
			}
		} else if t.State == "probing" {
			t.Desired = "inspect"
		}
		s.tasks[t.ID] = &t
	}
	go s.loop()
	return s, nil
}
func (s *Service) Close() error { s.cancel(); <-s.closed; return nil }
func (s *Service) do(ctx context.Context, fn func() (any, error)) (any, error) {
	r := call{ctx: ctx, fn: fn, reply: make(chan reply, 1)}
	select {
	case s.calls <- r:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.closed:
		return nil, model.Err("SERVICE_UNAVAILABLE", "service is stopped")
	}
	select {
	case v := <-r.reply:
		return v.value, v.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.closed:
		return nil, model.Err("SERVICE_UNAVAILABLE", "service is stopped")
	}
}
func (s *Service) loop() {
	defer close(s.closed)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	defer func() {
		for _, j := range s.jobs {
			j.cancel()
		}
		s.workers.Wait()
		s.registry.Close()
		s.store.Close()
	}()
	for {
		s.schedule()
		select {
		case <-s.ctx.Done():
			return
		case c := <-s.calls:
			if e := c.ctx.Err(); e != nil {
				c.reply <- reply{err: e}
			} else {
				v, e := c.fn()
				c.reply <- reply{v, e}
			}
		case u := <-s.updates:
			s.apply(u)
		case <-ticker.C:
			s.stalls()
		}
	}
}
func (s *Service) commit(t *model.Task, kind string) error {
	next := *t
	next.Revision++
	next.ObservedAt = time.Now().UTC()
	if err := s.store.Save(next, kind, "", nil); err != nil {
		return err
	}
	t.Revision = next.Revision
	t.ObservedAt = next.ObservedAt
	return nil
}
func (s *Service) record(t *model.Task, kind string) {
	if err := s.commit(t, kind); err != nil {
		if j := s.jobs[t.ID]; j != nil {
			j.stopping = true
			j.cancel()
		}
		t.State = "failed"
		t.Phase = ""
		t.Seeding = false
		t.Rate = 0
		t.ETA = nil
		t.Issue = model.Err("DISK_ERROR", "cannot persist task state; transfer stopped")
		if t.Metadata != nil {
			for i := range t.Metadata.Files {
				t.Metadata.Files[i].Ready = false
				t.Metadata.Files[i].Path = ""
			}
		}
	}
}

func (s *Service) emit(u update) {
	select {
	case s.updates <- u:
	case <-s.ctx.Done():
	}
}
func (s *Service) schedule() {
	var queued []*model.Task
	downloads, probes := 0, 0
	for id, j := range s.jobs {
		if j.probe {
			probes++
		} else {
			downloads++
		}
		t := s.tasks[id]
		if t != nil && t.Seeding {
			for _, other := range s.tasks {
				if other.State == "queued" && other.Desired == "running" {
					j.stopping = true
					j.cancel()
					t.Seeding = false
					t.State = "completed"
					s.record(t, "seeding_yielded")
					break
				}
			}
		}
	}
	for _, t := range s.tasks {
		if s.jobs[t.ID] == nil && (t.State == "probing" || t.State == "queued" || (t.State == "completed" && t.Policy.Seed && t.Desired == "running")) {
			queued = append(queued, t)
		}
	}
	sort.Slice(queued, func(i, j int) bool {
		a, b := queued[i], queued[j]
		if (a.State == "completed") != (b.State == "completed") {
			return b.State == "completed"
		}
		if model.PriorityRank(a.Policy.Priority) != model.PriorityRank(b.Policy.Priority) {
			return model.PriorityRank(a.Policy.Priority) < model.PriorityRank(b.Policy.Priority)
		}
		if !a.QueuedAt.Equal(b.QueuedAt) {
			return a.QueuedAt.Before(b.QueuedAt)
		}
		return a.ID < b.ID
	})
	for _, t := range queued {
		probe := t.Metadata == nil || t.State == "probing"
		if probe {
			if probes >= s.policy.Probes {
				continue
			}
			probes++
		} else {
			if downloads >= s.policy.Active {
				continue
			}
			downloads++
		}
		ctx, cancel := context.WithCancel(s.ctx)
		j := &job{cancel: cancel, probe: probe, lastAt: time.Now(), progressAt: time.Now()}
		s.jobs[t.ID] = j
		if probe {
			t.State = "probing"
			t.Phase = "metadata"
		} else {
			t.State = "checking"
			t.Phase = "checking"
			for i := range t.Metadata.Files {
				t.Metadata.Files[i].Ready = false
				t.Metadata.Files[i].Path = ""
			}
		}
		t.Issue = nil
		t.Seeding = false
		if e := s.commit(t, "started"); e != nil {
			delete(s.jobs, t.ID)
			cancel()
			t.State = "failed"
			t.Issue = model.Err("DISK_ERROR", "cannot persist task state")
			continue
		}
		copy := t.Copy()
		impl := s.registry.Engines[t.Source.Protocol]
		s.workers.Add(1)
		go func() {
			defer s.workers.Done()
			defer cancel()
			if probe {
				m, e := impl.Probe(ctx, copy.Source)
				s.emit(update{id: copy.ID, job: j, metadata: m, done: true, err: e})
				return
			}
			err := engine.RetryDownload(ctx, impl, copy, func(p engine.Progress) {
				p.Files = append([]model.File(nil), p.Files...)
				s.emit(update{id: copy.ID, job: j, progress: &p})
			})
			s.emit(update{id: copy.ID, job: j, done: true, err: err})
		}()
	}
}
func (s *Service) apply(u update) {
	j := s.jobs[u.id]
	t := s.tasks[u.id]
	if j != u.job || t == nil {
		return
	}
	if u.done {
		delete(s.jobs, u.id)
	}
	if j.stopping {
		return
	}
	if u.progress != nil {
		p := u.progress
		oldReady := map[int]bool{}
		for _, f := range t.Metadata.Files {
			oldReady[f.Index] = f.Ready
		}
		t.Metadata.Files = p.Files
		t.Seeding = p.Seeding
		t.Phase = p.Phase
		now := time.Now()
		delta := p.Transferred - j.lastBytes
		if delta < 0 {
			delta = p.Transferred
		}
		t.Transferred += delta
		elapsed := now.Sub(j.lastAt).Seconds()
		if elapsed > 0 {
			t.Rate = float64(delta) / elapsed
		}
		j.lastBytes = p.Transferred
		j.lastAt = now
		if delta > 0 {
			j.progressAt = now
			t.Issue = nil
		}
		t.ObservedAt = now.UTC()
		t.ETA = nil
		var remaining int64
		known := true
		all := true
		for _, f := range p.Files {
			if !f.Selected {
				continue
			}
			if !f.Ready {
				all = false
			}
			if f.Size == nil {
				known = false
			} else {
				remaining += *f.Size - f.Completed
			}
		}
		if known && t.Rate > 0 {
			eta := float64(max(remaining, 0)) / t.Rate
			t.ETA = &eta
		}
		if all {
			t.State = "completed"
			t.Phase = ""
		} else if p.Phase == "checking" {
			t.State = "checking"
		} else {
			t.State = "downloading"
		}
		for _, f := range p.Files {
			if f.Ready && !oldReady[f.Index] {
				s.record(t, "file_completed")
			}
		}
		return
	}
	if u.err != nil {
		t.State = "failed"
		t.Phase = ""
		t.Seeding = false
		t.Rate = 0
		t.ETA = nil
		t.Issue = model.AsError(u.err)
		s.record(t, "failed")
		return
	}
	if u.metadata != nil {
		t.Metadata = u.metadata
		t.Source.TorrentData = nil
		if t.Filename != "" && t.Source.Protocol != "torrent" {
			t.Metadata.Files[0].Name = t.Filename
		}
		if t.Metadata.InfoHash != "" {
			s.identities[t.Metadata.InfoHash] = t.ID
		}
		if t.StartAll {
			for i := range t.Metadata.Files {
				t.Metadata.Files[i].Selected = true
			}
			t.Desired = "running"
			t.State = "queued"
			t.QueuedAt = time.Now().UTC()
		} else {
			t.State = "ready"
			t.Desired = "inspect"
		}
		t.Phase = ""
		s.record(t, "probed")
		return
	}
	t.State = "completed"
	t.Phase = ""
	t.Seeding = false
	t.Rate = 0
	t.ETA = nil
	if !t.Policy.Seed {
		t.Desired = "stopped"
	}
	s.record(t, "completed")
}
func (s *Service) stalls() {
	for id, j := range s.jobs {
		t := s.tasks[id]
		if t == nil || j.stopping || t.Seeding {
			continue
		}
		if time.Since(j.progressAt) > 120*time.Second && t.Issue == nil {
			t.Issue = &model.Error{Code: "STALLED", Message: "no transfer progress observed for 120 seconds; availability is unknown", Retryable: true}
			s.record(t, "stalled")
		}
	}
}
func (s *Service) find(p model.Params) (*model.Task, error) {
	t := s.tasks[p.TaskID]
	if t == nil {
		return nil, model.Err("TASK_NOT_FOUND", "task does not exist")
	}
	if p.ExpectedRevision != nil && *p.ExpectedRevision != t.Revision {
		return nil, model.Err("REVISION_CONFLICT", "task changed; query it again")
	}
	return t, nil
}
func newID() string {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b[:])
}
func (s *Service) add(a model.Add, source model.Source, identity string) (any, error) {
	encoded, _ := json.Marshal(a)
	digest := sha256.Sum256(encoded)
	digestText := hex.EncodeToString(digest[:])
	if a.RequestKey != "" {
		r, e := s.store.Request(a.RequestKey)
		if e != nil {
			return nil, e
		}
		if r != nil {
			if r.Digest != digestText {
				return nil, model.Err("REQUEST_CONFLICT", "request key was used with different parameters")
			}
			if t := s.tasks[r.TaskID]; t != nil {
				return t.Public(), nil
			}
			return nil, model.Err("TASK_REMOVED", "the original task was removed; use a new request key")
		}
	}
	if identity != "" {
		if t := s.tasks[s.identities[identity]]; t != nil {
			if a.RequestKey != "" {
				if e := s.store.Save(*t, "", a.RequestKey, &store.Request{Digest: digestText, TaskID: t.ID}); e != nil {
					return nil, e
				}
			}
			return t.Public(), nil
		}
	}
	now := time.Now().UTC()
	t := &model.Task{ID: newID(), Revision: 1, Source: source, State: "probing", Desired: "inspect", Policy: model.TaskPolicy{Priority: "normal", Connections: 4}, SHA256: strings.ToLower(a.SHA256), Filename: a.Filename, StartAll: a.Start, CreatedAt: now, QueuedAt: now, ObservedAt: now}
	t.Dir = filepath.Join(s.root, t.ID)
	var request *store.Request
	if a.RequestKey != "" {
		request = &store.Request{Digest: digestText, TaskID: t.ID}
	}
	if e := s.store.Save(*t, "created", a.RequestKey, request); e != nil {
		return nil, e
	}
	s.tasks[t.ID] = t
	if identity != "" {
		s.identities[identity] = t.ID
	}
	return t.Public(), nil
}
func (s *Service) Call(ctx context.Context, method string, raw json.RawMessage) (any, error) {
	switch method {
	case "task_add", "task_get", "task_list", "task_files", "task_select", "task_policy_set", "task_start", "task_pause", "task_resume", "task_restart", "task_remove", "task_wait", "events_list", "policy_get", "policy_set", "service_status":
	default:
		return nil, model.Err("METHOD_NOT_FOUND", "unknown operation")
	}
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	if method == "task_add" {
		var a model.Add
		if e := decode(raw, &a); e != nil {
			return nil, e
		}
		var source model.Source
		var e error
		if len(a.TorrentData) > 0 {
			if len(a.TorrentData) > model.MaxTorrentSize {
				return nil, model.Err("INVALID_TORRENT", "torrent upload exceeds 8 MiB")
			}
			if strings.Contains(a.Source, "://") || strings.HasPrefix(a.Source, "magnet:") {
				return nil, model.Err("INVALID_ARGUMENT", "torrent_data cannot be combined with a network source")
			}
			source = model.Source{Protocol: "torrent", TorrentData: a.TorrentData, TorrentPath: a.Source}
		} else {
			source, e = model.ParseSource(a.Source)
		}
		if e != nil {
			return nil, e
		}
		source.CredentialRef = a.CredentialRef
		if a.CredentialRef != "" {
			if _, ok := s.credentials[a.CredentialRef]; !ok {
				return nil, model.Err("CREDENTIAL_NOT_FOUND", "credential reference is not configured")
			}
		}
		if a.Filename != "" && !engine.SafeOutputName(a.Filename) {
			return nil, model.Err("INVALID_PATH", "filename must be a basename")
		}
		if a.SHA256 != "" {
			b, e := hex.DecodeString(a.SHA256)
			if e != nil || len(b) != 32 {
				return nil, model.Err("INVALID_ARGUMENT", "sha256 must contain 64 hexadecimal characters")
			}
		}
		identity := ""
		if source.Protocol == "torrent" {
			if a.SHA256 != "" || a.Filename != "" {
				return nil, model.Err("UNSUPPORTED_CAPABILITY", "torrent tasks use embedded hashes and filenames")
			}
			identity, e = engine.TorrentIdentity(source)
			if e != nil {
				return nil, e
			}
		}
		return s.do(ctx, func() (any, error) { return s.add(a, source, identity) })
	}
	var p model.Params
	if e := decode(raw, &p); e != nil {
		return nil, e
	}
	if p.Limit == 0 {
		p.Limit = 50
	}
	if p.Limit < 1 || p.Limit > 200 || p.Offset < 0 {
		return nil, model.Err("INVALID_ARGUMENT", "limit must be 1..200 and offset >= 0")
	}
	if method == "task_wait" {
		return s.wait(ctx, p)
	}
	return s.do(ctx, func() (any, error) {
		switch method {
		case "task_list":
			list := []model.Task{}
			for _, t := range s.tasks {
				if p.State == "" || p.State == t.State {
					public := t.Public()
					if public.Metadata != nil {
						public.Metadata.Files = nil
					}
					list = append(list, public)
				}
			}
			sort.Slice(list, func(i, j int) bool { return list[i].CreatedAt.Before(list[j].CreatedAt) })
			start := min(p.Offset, len(list))
			end := min(len(list), start+p.Limit)
			return map[string]any{"tasks": list[start:end], "total": len(list), "next_offset": end}, nil
		case "policy_get":
			return s.policy, nil
		case "policy_set":
			if p.ResourcePolicy == nil {
				return nil, model.Err("INVALID_ARGUMENT", "resource_policy is required")
			}
			if e := p.ResourcePolicy.Validate(); e != nil {
				return nil, e
			}
			if e := s.store.Policy(*p.ResourcePolicy); e != nil {
				return nil, e
			}
			s.policy = *p.ResourcePolicy
			s.registry.SetPolicy(s.policy)
			return s.policy, nil
		case "events_list":
			events, cursor, e := s.store.Events(p.Cursor, p.TaskID, p.Limit)
			return map[string]any{"events": events, "next_cursor": cursor}, e
		case "service_status":
			active, probes, queued, pendingProbes := 0, 0, 0, 0
			for _, j := range s.jobs {
				if j.probe {
					probes++
				} else {
					active++
				}
			}
			for _, task := range s.tasks {
				if task.State == "queued" {
					queued++
				}
				if task.State == "probing" && s.jobs[task.ID] == nil {
					pendingProbes++
				}
			}
			return map[string]any{"version": model.Version, "tasks": len(s.tasks), "workers": len(s.jobs), "active_downloads": active, "active_probes": probes, "queued_downloads": queued, "pending_probes": pendingProbes, "policy": s.policy}, nil
		}
		t, e := s.find(p)
		if e != nil {
			return nil, e
		}
		if method != "task_get" && method != "task_files" {
			copy := t.Copy()
			t = &copy
		}
		switch method {
		case "task_get":
			return t.Public(), nil
		case "task_files":
			if t.Metadata == nil {
				return nil, model.Err("METADATA_PENDING", "source is still being probed")
			}
			files := append([]model.File(nil), t.Metadata.Files...)
			for i := range files {
				files[i] = files[i].Public()
			}
			start := min(p.Offset, len(files))
			end := min(len(files), start+p.Limit)
			return map[string]any{"files": files[start:end], "total": len(files), "next_offset": end}, nil
		case "task_pause":
			t.Desired = "paused"
			t.State = "paused"
			t.Phase = ""
			t.Seeding = false
			t.Rate = 0
			t.ETA = nil
		case "task_start", "task_resume":
			if j := s.jobs[t.ID]; j != nil && (!j.probe || j.stopping) {
				return nil, model.Err("TASK_BUSY", "task is running or stopping")
			}
			if t.Metadata == nil && t.Source.Protocol == "torrent" && !t.StartAll {
				return nil, model.Err("METADATA_PENDING", "wait for torrent metadata and select files before starting")
			}
			if t.Metadata == nil {
				t.State = "probing"
				t.Desired = "inspect"
				t.StartAll = true
			} else {
				selected := false
				for _, f := range t.Metadata.Files {
					selected = selected || f.Selected
				}
				if !selected {
					return nil, model.Err("EMPTY_SELECTION", "select files before starting")
				}
				t.State = "queued"
				t.Desired = "running"
				t.QueuedAt = time.Now().UTC()
				t.Issue = nil
			}
		case "task_select":
			if t.Source.Protocol != "torrent" {
				return nil, model.Err("UNSUPPORTED_CAPABILITY", "file selection is only available for torrent tasks")
			}
			if t.Metadata == nil {
				return nil, model.Err("METADATA_PENDING", "wait for torrent metadata")
			}
			if s.jobs[t.ID] != nil || t.State == "queued" {
				return nil, model.Err("TASK_BUSY", "pause the task before replacing its selection")
			}
			selected := map[int]string{}
			for _, f := range p.Files {
				if f.Index < 0 || f.Index >= len(t.Metadata.Files) || selected[f.Index] != "" {
					return nil, model.Err("INVALID_ARGUMENT", "invalid or duplicate file index")
				}
				priority := f.Priority
				if priority == "" {
					priority = "normal"
				}
				if priority != "normal" && priority != "high" {
					return nil, model.Err("INVALID_ARGUMENT", "file priority must be normal or high")
				}
				selected[f.Index] = priority
			}
			for i := range t.Metadata.Files {
				f := &t.Metadata.Files[i]
				f.Selected = selected[i] != ""
				if f.Selected {
					f.Priority = selected[i]
				}
			}
			t.State = "ready"
			t.Desired = "inspect"
		case "task_policy_set":
			if p.TaskPolicy == nil {
				return nil, model.Err("INVALID_ARGUMENT", "policy is required")
			}
			policy := *p.TaskPolicy
			if model.PriorityRank(policy.Priority) == 3 || policy.Connections < 1 || policy.Connections > 4 {
				return nil, model.Err("INVALID_ARGUMENT", "priority must be high/normal/low and connections 1..4")
			}
			if policy.Seed && t.Source.Protocol != "torrent" {
				return nil, model.Err("UNSUPPORTED_CAPABILITY", "seeding requires a torrent")
			}
			if policy.Connections != t.Policy.Connections && s.jobs[t.ID] != nil {
				return nil, model.Err("TASK_BUSY", "pause before changing transfer concurrency")
			}
			if policy.Seed != t.Policy.Seed && s.jobs[t.ID] != nil {
				return nil, model.Err("TASK_BUSY", "pause before changing seeding policy")
			}
			t.Policy = policy
		case "task_restart":
			if s.jobs[t.ID] != nil {
				return nil, model.Err("TASK_BUSY", "pause and wait for transfer shutdown before restarting")
			}
			if t.Source.Protocol == "torrent" {
				return nil, model.Err("UNSUPPORTED_CAPABILITY", "torrent recovery rechecks pieces; use resume")
			}
			r, e := os.OpenRoot(t.Dir)
			if e == nil {
				for _, name := range []string{"content.part", "checkpoint.json", "checkpoint.tmp"} {
					if e = r.Remove(name); e != nil && !os.IsNotExist(e) {
						r.Close()
						return nil, model.Err("DISK_ERROR", "cannot discard old checkpoint")
					}
				}
				r.Close()
			} else if !os.IsNotExist(e) {
				return nil, model.Err("DISK_ERROR", "cannot open task directory")
			}
			t.Metadata = nil
			t.State = "probing"
			t.Desired = "inspect"
			t.StartAll = true
			t.Issue = nil
		case "task_remove":
			if s.jobs[t.ID] != nil {
				return nil, model.Err("TASK_BUSY", "pause and wait for transfer shutdown before removing")
			}
			if e = s.store.Save(*t, "removed", "", nil); e != nil {
				return nil, e
			}
			delete(s.tasks, t.ID)
			for identity, id := range s.identities {
				if id == t.ID {
					delete(s.identities, identity)
				}
			}
			return map[string]any{"removed": t.ID, "data_preserved": true}, nil
		default:
			return nil, model.Err("METHOD_NOT_FOUND", "unknown operation")
		}
		if e = s.commit(t, method); e != nil {
			return nil, model.Err("DISK_ERROR", "cannot persist task intent")
		}
		s.tasks[t.ID] = t
		if method == "task_pause" {
			if j := s.jobs[t.ID]; j != nil {
				j.stopping = true
				j.cancel()
			}
		}
		return t.Public(), nil
	})
}
func (s *Service) wait(ctx context.Context, p model.Params) (any, error) {
	if p.TimeoutSeconds == 0 {
		p.TimeoutSeconds = 20
	}
	if p.TimeoutSeconds < 1 || p.TimeoutSeconds > 30 {
		return nil, model.Err("INVALID_ARGUMENT", "timeout_seconds must be 1..30")
	}
	if p.Condition == "" {
		p.Condition = "completed"
	}
	if p.Condition != "metadata" && p.Condition != "completed" && p.Condition != "files" && p.Condition != "idle" {
		return nil, model.Err("INVALID_ARGUMENT", "condition must be metadata, completed, files or idle")
	}
	if p.Condition == "files" && len(p.FileIndices) == 0 {
		return nil, model.Err("INVALID_ARGUMENT", "file_indices are required")
	}
	timeout := time.NewTimer(time.Duration(p.TimeoutSeconds) * time.Second)
	defer timeout.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		v, e := s.do(ctx, func() (any, error) {
			t, e := s.find(p)
			if e != nil {
				return nil, e
			}
			matched := false
			switch p.Condition {
			case "metadata":
				matched = t.Metadata != nil
			case "completed":
				matched = t.State == "completed"
			case "idle":
				matched = s.jobs[t.ID] == nil
			case "files":
				if t.Metadata != nil {
					matched = true
					for _, i := range p.FileIndices {
						if i < 0 || i >= len(t.Metadata.Files) {
							return nil, model.Err("INVALID_ARGUMENT", "invalid file index")
						}
						matched = matched && t.Metadata.Files[i].Ready
					}
				}
			}
			return map[string]any{"task": t.Public(), "matched": matched, "timed_out": false}, nil
		})
		if e != nil {
			return nil, e
		}
		result := v.(map[string]any)
		if result["matched"].(bool) || result["task"].(model.Task).State == "failed" {
			return result, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout.C:
			result["timed_out"] = true
			return result, nil
		case <-ticker.C:
		}
	}
}
func decode(raw []byte, out any) error {
	if !strings.HasPrefix(strings.TrimSpace(string(raw)), "{") {
		return model.Err("INVALID_ARGUMENT", "arguments must be a JSON object")
	}
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.DisallowUnknownFields()
	if e := d.Decode(out); e != nil {
		return model.Err("INVALID_ARGUMENT", fmt.Sprintf("invalid arguments: %v", e))
	}
	if e := d.Decode(new(any)); e != io.EOF {
		return model.Err("INVALID_ARGUMENT", "arguments must contain exactly one JSON object")
	}
	return nil
}

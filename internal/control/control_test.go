package control

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RyanChen-cn/fetchra/internal/model"
	"github.com/RyanChen-cn/fetchra/internal/service"
	"github.com/RyanChen-cn/fetchra/internal/transport"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const token = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

type testNode struct {
	server  *httptest.Server
	service *service.Service
	cfg     NodeConfig
}

func startNode(t *testing.T, name string) testNode {
	t.Helper()
	return startNodeWithHandler(t, name, nil)
}
func startNodeWithHandler(t *testing.T, name string, wrap func(*service.Service, http.Handler) http.Handler) testNode {
	t.Helper()
	root := t.TempDir()
	s, err := service.New(filepath.Join(root, "state"), filepath.Join(root, "downloads"), nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := transport.NodeHandler(s)
	if wrap != nil {
		handler = wrap(s, handler)
	}
	server := httptest.NewTLSServer(transport.AuthenticatedHandler(token, handler))
	t.Cleanup(func() { server.Close(); s.Close() })
	auth := filepath.Join(root, "token")
	os.WriteFile(auth, []byte(token), 0600)
	ca := filepath.Join(root, "ca.pem")
	os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600)
	return testNode{server, s, NodeConfig{Name: name, Endpoint: server.URL, TokenFile: auth, CAFile: ca}}
}
func config(t *testing.T, nodes ...testNode) Config {
	t.Helper()
	root := t.TempDir()
	f := NodeFile{Default: "auto"}
	for _, node := range nodes {
		f.Nodes = append(f.Nodes, node.cfg)
	}
	data, _ := json.Marshal(f)
	path := filepath.Join(root, "nodes.json")
	os.WriteFile(path, data, 0600)
	return Config{NodesFile: path, StateDir: filepath.Join(root, "state"), FetchDir: filepath.Join(root, "fetched")}
}
func call(t *testing.T, c *Controller, method string, args any) any {
	t.Helper()
	raw, _ := json.Marshal(args)
	v, err := c.Call(context.Background(), method, raw)
	if err != nil {
		t.Fatal(method, err)
	}
	return v
}
func taskID(t *testing.T, v any) string {
	t.Helper()
	id, ok := v.(map[string]any)["task_id"].(string)
	if !ok {
		t.Fatal("missing task id")
	}
	return id
}
func TestNodeSchedulingPersistenceAndFetch(t *testing.T) {
	a, b := startNode(t, "a"), startNode(t, "b")
	cfg := config(t, a, b)
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	call(t, c, "policy_set", map[string]any{"node": "a", "resource_policy": model.Policy{Active: 1, Probes: 2, PerHost: 4}})
	blocker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			w.Header().Set("Content-Length", "1048576")
			return
		}
		if r.Header.Get("Range") == "bytes=0-0" {
			w.Header().Set("Content-Length", "1048576")
			w.WriteHeader(200)
			return
		}
		w.Header().Set("Content-Length", "1048576")
		w.WriteHeader(200)
		w.Write(bytes.Repeat([]byte("x"), 4096))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	t.Cleanup(blocker.Close)
	busy := taskID(t, call(t, c, "task_add", map[string]any{"node": "a", "source": blocker.URL + "/busy", "start": true, "request_key": "busy"}))
	// Always pause before closing the fixture, including failure paths.
	t.Cleanup(func() {
		raw, _ := json.Marshal(model.Params{TaskID: strings.TrimPrefix(busy, "a:")})
		a.service.Call(context.Background(), "task_pause", raw)
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		v := call(t, c, "task_get", map[string]any{"task_id": busy}).(map[string]any)
		if v["state"] == "downloading" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first node never occupied a download slot")
		}
		time.Sleep(10 * time.Millisecond)
	}
	payload := []byte("independent node download")
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"v1"`)
		http.ServeContent(w, r, "result.bin", time.Time{}, bytes.NewReader(payload))
	}))
	defer origin.Close()
	request := map[string]any{"source": origin.URL + "/result.bin", "start": true, "request_key": "assigned"}
	id := taskID(t, call(t, c, "task_add", request))
	if !strings.HasPrefix(id, "b:") {
		t.Fatal("auto ignored node load", id)
	}
	call(t, c, "task_wait", map[string]any{"task_id": id, "timeout_seconds": 5})
	file := call(t, c, "task_fetch", map[string]any{"task_id": id}).(model.File)
	data, err := os.ReadFile(file.Path)
	if err != nil || !bytes.Equal(data, payload) {
		t.Fatal("node file not fetched", err)
	}
	c.Close()
	c, err = New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if again := taskID(t, call(t, c, "task_add", request)); again != id {
		t.Fatal("controller restart lost task assignment")
	}
	// A key is global in a pool; explicit retargeting is a conflict.
	explicit := map[string]any{"node": "a", "source": origin.URL + "/result.bin", "start": true, "request_key": "assigned"}
	raw, _ := json.Marshal(explicit)
	if _, err = c.Call(context.Background(), "task_add", raw); model.AsError(err).Code != "REQUEST_CONFLICT" {
		t.Fatal("key moved to another node", err)
	}
	b.server.Close()
	raw, _ = json.Marshal(request)
	if _, err = c.Call(context.Background(), "task_add", raw); err == nil {
		t.Fatal("offline assigned node was silently replaced")
	}
	health := call(t, c, "node_list", nil).([]Health)
	if len(health) != 2 || health[1].Error == nil {
		t.Fatal("offline node hidden", health)
	}
	call(t, c, "task_pause", map[string]any{"task_id": busy})
	call(t, c, "task_wait", map[string]any{"task_id": busy, "condition": "idle", "timeout_seconds": 5})
}
func TestConcurrentAssignmentAndConflict(t *testing.T) {
	a, b := startNode(t, "a"), startNode(t, "b")
	cfg := config(t, a, b)
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "tiny", time.Time{}, bytes.NewReader([]byte("x")))
	}))
	defer origin.Close()
	request := model.Add{Source: origin.URL + "/tiny", RequestKey: "concurrent"}
	raw, _ := json.Marshal(request)
	ids := make(chan string, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := c.Call(context.Background(), "task_add", raw)
			if err != nil {
				t.Error(err)
				return
			}
			ids <- v.(map[string]any)["task_id"].(string)
		}()
	}
	wg.Wait()
	close(ids)
	first := ""
	count := 0
	for id := range ids {
		count++
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatal("duplicate node assignments", first, id)
		}
	}
	if count != 8 {
		t.Fatal("missing concurrent replies", count)
	}
	total := 0
	for _, name := range []string{"a", "b"} {
		v := call(t, c, "task_list", map[string]any{"node": name}).(map[string]any)
		n, _ := v["total"].(json.Number).Int64()
		total += int(n)
	}
	if total != 1 {
		t.Fatal("one request created tasks on multiple nodes", total)
	}
	changed := model.Add{Source: origin.URL + "/other", RequestKey: "concurrent"}
	raw, _ = json.Marshal(changed)
	if _, err = c.Call(context.Background(), "task_add", raw); model.AsError(err).Code != "REQUEST_CONFLICT" {
		t.Fatal("changed request accepted", err)
	}
	cfg.Node = "auto"
	noKey, _ := json.Marshal(model.Add{Source: origin.URL})
	if _, err = c.Call(context.Background(), "task_add", noKey); model.AsError(err).Code != "REQUEST_KEY_REQUIRED" {
		t.Fatal("auto request lacked stable identity", err)
	}
	raw = []byte(`{"node":"a","task_id":"b:fake"}`)
	if _, err = c.Call(context.Background(), "task_get", raw); model.AsError(err).Code != "INVALID_ARGUMENT" {
		t.Fatal("conflicting node and task id accepted", err)
	}
}
func TestMCPRemoteWorkflow(t *testing.T) {
	node := startNode(t, "primary")
	cfg := config(t, node)
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	payload := []byte("MCP remote result")
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "mcp.bin", time.Time{}, bytes.NewReader(payload))
	}))
	defer origin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ct, st := mcp.NewInMemoryTransports()
	server, err := transport.MCPServer(c).Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	tool := func(name string, args map[string]any) json.RawMessage {
		t.Helper()
		result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
		if err != nil || result.IsError {
			t.Fatal(name, result, err)
		}
		data, _ := json.Marshal(result.StructuredContent)
		var envelope struct {
			Result json.RawMessage `json:"result"`
		}
		json.Unmarshal(data, &envelope)
		return envelope.Result
	}
	var task struct {
		ID string `json:"task_id"`
	}
	json.Unmarshal(tool("task_add", map[string]any{"source": origin.URL + "/mcp.bin", "start": true, "request_key": "mcp"}), &task)
	if !strings.HasPrefix(task.ID, "primary:") {
		t.Fatal("MCP task lacks node identity", task)
	}
	tool("task_wait", map[string]any{"task_id": task.ID, "timeout_seconds": 5})
	var file model.File
	json.Unmarshal(tool("task_fetch", map[string]any{"task_id": task.ID, "file_index": 0}), &file)
	data, err := os.ReadFile(file.Path)
	if err != nil || !bytes.Equal(data, payload) {
		t.Fatal("MCP did not retrieve file", err)
	}
	tool("node_list", nil)
}

func TestLocalTorrentUploadedAndSurvivesSourceRemoval(t *testing.T) {
	node := startNode(t, "seedbox")
	cfg := config(t, node)
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	payload := filepath.Join(t.TempDir(), "payload.bin")
	if err := os.WriteFile(payload, []byte("metainfo upload"), 0600); err != nil {
		t.Fatal(err)
	}
	info := metainfo.Info{PieceLength: 16 << 10}
	if err := info.BuildFromFilePath(payload); err != nil {
		t.Fatal(err)
	}
	data, err := bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	mi := metainfo.MetaInfo{InfoBytes: data}
	var encoded bytes.Buffer
	if err := mi.Write(&encoded); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "local.torrent")
	if err := os.WriteFile(path, encoded.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	result := call(t, c, "task_add", map[string]any{"source": path, "request_key": "uploaded"})
	id := taskID(t, result)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	waited := call(t, c, "task_wait", map[string]any{"task_id": id, "condition": "metadata", "timeout_seconds": 5}).(map[string]any)
	if waited["matched"] != true {
		t.Fatal("uploaded metainfo depended on client file")
	}
	task := waited["task"].(map[string]any)
	source := task["source"].(map[string]any)
	if source["torrent_data"] != nil {
		t.Fatal("raw uploaded metainfo leaked in snapshot")
	}
	metadata := task["metadata"].(map[string]any)
	if metadata["info_hash"] == "" || len(metadata["files"].([]any)) != 1 {
		t.Fatal("wrong uploaded metadata")
	}
}

func TestLostReplyKeepsAssignmentAndDeduplicates(t *testing.T) {
	var dropped atomic.Bool
	node := startNodeWithHandler(t, "owner", func(s *service.Service, ordinary http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			var request transport.Request
			json.Unmarshal(body, &request)
			if request.Method == "task_add" && dropped.CompareAndSwap(false, true) {
				if _, err := s.Call(r.Context(), request.Method, request.Params); err != nil {
					t.Error(err)
					return
				}
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				conn.Close()
				return
			}
			ordinary.ServeHTTP(w, r)
		})
	})
	cfg := config(t, node)
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "source", time.Time{}, strings.NewReader("retry after lost reply"))
	}))
	defer origin.Close()
	request := model.Add{Source: origin.URL + "/source", RequestKey: "ambiguous"}
	raw, _ := json.Marshal(request)
	if _, err := c.Call(context.Background(), "task_add", raw); err == nil {
		t.Fatal("lost reply was not reported")
	}
	c.Close()
	c, err = New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	id := taskID(t, call(t, c, "task_add", request))
	if !strings.HasPrefix(id, "owner:") {
		t.Fatal("lost assignment after ambiguous response", id)
	}
	listed := call(t, c, "task_list", map[string]any{"node": "owner"}).(map[string]any)
	total, _ := listed["total"].(json.Number).Int64()
	if total != 1 {
		t.Fatal("retry duplicated remote task", total)
	}
	// Removing the owner from configuration must not choose another live node.
	replacement := startNode(t, "replacement")
	changed := NodeFile{Default: "auto", Nodes: []NodeConfig{replacement.cfg}}
	data, _ := json.Marshal(changed)
	os.WriteFile(cfg.NodesFile, data, 0600)
	other, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.Call(context.Background(), "task_add", raw); model.AsError(err).Code != "NODE_CONFIG_CHANGED" {
		t.Fatal("removed owner was silently replaced", err)
	}
}

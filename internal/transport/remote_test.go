package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"github.com/RyanChen-cn/fetchra/internal/model"
	"github.com/RyanChen-cn/fetchra/internal/service"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testToken = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func rpcCall(t *testing.T, c *Client, method string, args any) any {
	t.Helper()
	raw, _ := json.Marshal(args)
	v, err := c.Call(context.Background(), method, raw)
	if err != nil {
		t.Fatal(method, err)
	}
	return v
}
func decodeTask(t *testing.T, v any) model.Task {
	t.Helper()
	data, _ := json.Marshal(v)
	var task model.Task
	if err := json.Unmarshal(data, &task); err != nil {
		t.Fatal(err)
	}
	return task
}
func TestRemoteTLSAuthentication(t *testing.T) {
	node := httptest.NewTLSServer(AuthenticatedHandler(testToken, Handler(stub{})))
	defer node.Close()
	cert := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: node.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	client, err := NewRemoteClient(RemoteClientConfig{Endpoint: node.URL, Token: testToken, CAFile: cert})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	rpcCall(t, client, "service_status", nil)
	wrong, err := NewRemoteClient(RemoteClientConfig{Endpoint: node.URL, Token: strings.Repeat("x", 64), CAFile: cert})
	if err != nil {
		t.Fatal(err)
	}
	defer wrong.Close()
	if _, err = wrong.Call(context.Background(), "service_status", nil); model.AsError(err).Code != "AUTH_FAILED" {
		t.Fatal("wrong token accepted", err)
	}
	untrusted, err := NewRemoteClient(RemoteClientConfig{Endpoint: node.URL, Token: testToken})
	if err != nil {
		t.Fatal(err)
	}
	defer untrusted.Close()
	if _, err = untrusted.Call(context.Background(), "service_status", nil); err == nil {
		t.Fatal("untrusted TLS certificate accepted")
	}
	req := httptest.NewRequest("POST", "/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"service_status"}`))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Origin", "https://another.example")
	w := httptest.NewRecorder()
	AuthenticatedHandler(testToken, Handler(stub{})).ServeHTTP(w, req)
	if w.Code != 403 {
		t.Fatal("browser origin accepted")
	}
	for _, endpoint := range []string{"http://192.0.2.1:8080", "https://user:password@example.org", "https://example.org/path", "https://example.org?secret=x"} {
		if c, err := NewRemoteClient(RemoteClientConfig{Endpoint: endpoint, Token: testToken}); err == nil {
			c.Close()
			t.Fatal("unsafe endpoint accepted", endpoint)
		}
	}
}
func TestTokenAndRemoteListenerConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	os.WriteFile(path, []byte(testToken+"\n"), 0600)
	if token, err := ReadToken(path); err != nil || token != testToken {
		t.Fatal(err)
	}
	os.Chmod(path, 0644)
	if _, err := ReadToken(path); err == nil {
		t.Fatal("public token file accepted")
	}
	for _, cfg := range []RemoteConfig{{Listen: "0.0.0.0:0", Token: testToken}, {Listen: "127.0.0.1:0", Token: "short"}, {Listen: "127.0.0.1:0", Token: testToken, CertFile: "missing"}} {
		socket := filepath.Join(t.TempDir(), "service.sock")
		if err := ServeWithRemote(context.Background(), socket, stub{}, cfg); err == nil {
			t.Fatal("invalid remote configuration accepted")
		}
		if _, err := os.Stat(socket); !os.IsNotExist(err) {
			t.Fatal("failed config created socket", err)
		}
	}
}
func TestRemoteControlAndVerifiedFetch(t *testing.T) {
	payload := bytes.Repeat([]byte("remote!"), 100000)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"origin"`)
		http.ServeContent(w, r, "file.bin", time.Time{}, bytes.NewReader(payload))
	}))
	defer origin.Close()
	root := t.TempDir()
	s, err := service.New(filepath.Join(root, "state"), filepath.Join(root, "downloads"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	node := httptest.NewServer(AuthenticatedHandler(testToken, NodeHandler(s)))
	defer node.Close()
	client, err := NewRemoteClient(RemoteClientConfig{Endpoint: node.URL, Token: testToken})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	task := decodeTask(t, rpcCall(t, client, "task_add", model.Add{Source: origin.URL + "/file.bin", Start: true, RequestKey: "remote"}))
	rpcCall(t, client, "task_wait", model.Params{TaskID: task.ID, TimeoutSeconds: 10})
	got, err := client.Fetch(context.Background(), task.ID, 0, filepath.Join(root, "fetched"))
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(got.Path)
	if err != nil || !bytes.Equal(data, payload) || got.Integrity != "sha256_verified" {
		t.Fatal("bad retrieved file", err, got)
	}
	again, err := client.Fetch(context.Background(), task.ID, 0, filepath.Join(root, "fetched"))
	if err != nil || again.Path != got.Path {
		t.Fatal("repeat fetch did not recover output", err)
	}
	// Unauthenticated file access must not reveal either paths or payload bytes.
	resp, err := http.Get(node.URL + "/files/" + task.ID + "/0")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatal("file endpoint was public")
	}
	current := decodeTask(t, rpcCall(t, client, "task_get", model.Params{TaskID: task.ID}))
	if err := os.WriteFile(current.Metadata.Files[0].Path, bytes.Repeat([]byte("x"), len(payload)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Fetch(context.Background(), task.ID, 0, filepath.Join(root, "other")); err == nil {
		t.Fatal("changed source file exported")
	}
}
func TestRPCTrailingRequestRejected(t *testing.T) {
	w := httptest.NewRecorder()
	Handler(stub{}).ServeHTTP(w, httptest.NewRequest("POST", "/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"service_status"} {}`)))
	var r Response
	json.Unmarshal(w.Body.Bytes(), &r)
	if r.Error == nil || r.Error.Code != -32700 {
		t.Fatal("trailing request was accepted")
	}
}

func TestExportRejectsTaskDirectoryEscape(t *testing.T) {
	root := t.TempDir()
	s, err := service.New(filepath.Join(root, "state"), filepath.Join(root, "downloads"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "payload", time.Time{}, strings.NewReader("data"))
	}))
	defer origin.Close()
	raw, _ := json.Marshal(model.Add{Source: origin.URL + "/payload", Start: true})
	v, err := s.Call(context.Background(), "task_add", raw)
	if err != nil {
		t.Fatal(err)
	}
	task := v.(model.Task)
	raw, _ = json.Marshal(model.Params{TaskID: task.ID, Condition: "idle", TimeoutSeconds: 5})
	// Wait for completion, then for the worker to release its files.
	completed, _ := json.Marshal(model.Params{TaskID: task.ID, TimeoutSeconds: 5})
	if _, err := s.Call(context.Background(), "task_wait", completed); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Call(context.Background(), "task_wait", raw); err != nil {
		t.Fatal(err)
	}
	original := filepath.Join(root, "downloads", task.ID)
	outside := filepath.Join(t.TempDir(), "moved")
	if err := os.Rename(original, outside); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, original); err != nil {
		t.Fatal(err)
	}
	if f, _, err := s.OpenFile(context.Background(), task.ID, 0); err == nil {
		f.Close()
		t.Fatal("task directory symlink escaped download root")
	}
}

func TestFetchChecksSHAWhenLocalTimestampIsRestored(t *testing.T) {
	root := t.TempDir()
	s, err := service.New(filepath.Join(root, "state"), filepath.Join(root, "downloads"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "file", time.Time{}, strings.NewReader("good data"))
	}))
	defer origin.Close()
	node := httptest.NewServer(AuthenticatedHandler(testToken, NodeHandler(s)))
	defer node.Close()
	client, err := NewRemoteClient(RemoteClientConfig{Endpoint: node.URL, Token: testToken})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	task := decodeTask(t, rpcCall(t, client, "task_add", model.Add{Source: origin.URL + "/file", Start: true}))
	rpcCall(t, client, "task_wait", model.Params{TaskID: task.ID, TimeoutSeconds: 5})
	task = decodeTask(t, rpcCall(t, client, "task_get", model.Params{TaskID: task.ID}))
	path := task.Metadata.Files[0].Path
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("evil data"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	// Even deliberate metadata restoration cannot trick the receiving client into
	// publishing bytes that differ from the original completion's SHA-256.
	if _, err := client.Fetch(context.Background(), task.ID, 0, filepath.Join(root, "fetched")); model.AsError(err).Code != "CHECKSUM_MISMATCH" {
		t.Fatal("incorrect bytes were trusted", err)
	}
}

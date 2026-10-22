package main

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/RyanChen-cn/fetchra/internal/engine"
	"github.com/RyanChen-cn/fetchra/internal/model"
)

func cli(t *testing.T, state string, args ...string) json.RawMessage {
	t.Helper()
	var out bytes.Buffer
	args = append([]string{"--state", state}, args...)
	if e := run(context.Background(), args, &out, io.Discard); e != nil {
		t.Fatalf("CLI %v: %v (%s)", args, e, out.String())
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
	}
	if e := json.Unmarshal(out.Bytes(), &envelope); e != nil {
		t.Fatal(e)
	}
	return envelope.Result
}
func startInProcess(t *testing.T, state string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, []string{"--state", state, "serve"}, io.Discard, io.Discard) }()
	t.Cleanup(func() {
		cancel()
		select {
		case e := <-done:
			if e != nil {
				t.Error(e)
			}
		case <-time.After(10 * time.Second):
			t.Error("service did not stop")
		}
	})
	waitSocket(t, state)
}
func waitSocket(t *testing.T, state string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var out bytes.Buffer
		if e := run(context.Background(), []string{"--state", state, "status", "--json"}, &out, io.Discard); e == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("service socket never became available")
}
func TestCLIWorkflow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "test.txt", time.Time{}, strings.NewReader("CLI output"))
	}))
	defer server.Close()
	state := filepath.Join(t.TempDir(), "state")
	startInProcess(t, state)
	var task model.Task
	if e := json.Unmarshal(cli(t, state, "task", "add", "--json", "--start", server.URL+"/test.txt"), &task); e != nil {
		t.Fatal(e)
	}
	var result struct {
		Task    model.Task `json:"task"`
		Matched bool       `json:"matched"`
	}
	if e := json.Unmarshal(cli(t, state, "task", "wait", "--json", "--id", task.ID, "--timeout", "5"), &result); e != nil {
		t.Fatal(e)
	}
	if !result.Matched || !result.Task.Metadata.Files[0].Ready {
		t.Fatal("CLI did not return completed file")
	}
	data, e := os.ReadFile(result.Task.Metadata.Files[0].Path)
	if e != nil || string(data) != "CLI output" {
		t.Fatal("wrong output", e)
	}
	var out bytes.Buffer
	e = run(context.Background(), []string{"--state", state, "task", "get", "--json", "--id", "missing"}, &out, io.Discard)
	if e == nil || !strings.Contains(out.String(), `"TASK_NOT_FOUND"`) {
		t.Fatal("CLI failure is not structured", out.String(), e)
	}
}

func TestHelperDaemon(t *testing.T) {
	if os.Getenv("FETCHRA_TEST_HELPER") != "1" {
		return
	}
	args := []string{"--state", os.Getenv("FETCHRA_TEST_STATE"), "serve"}
	if raw := os.Getenv("FETCHRA_TEST_ARGS"); raw != "" {
		var extra []string
		if err := json.Unmarshal([]byte(raw), &extra); err != nil {
			os.Exit(2)
		}
		args = append(args, extra...)
	}
	if e := run(context.Background(), args, io.Discard, os.Stderr); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(2)
	}
	os.Exit(0)
}
func helperDaemon(t *testing.T, state string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperDaemon$")
	cmd.Env = append(os.Environ(), "FETCHRA_TEST_HELPER=1", "FETCHRA_TEST_STATE="+state)
	if e := cmd.Start(); e != nil {
		t.Fatal(e)
	}
	waitSocket(t, state)
	return cmd
}
func TestCrashRecovery(t *testing.T) {
	data := bytes.Repeat([]byte("recover!"), 3*int(engine.BlockSize)/8+13)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"stable"`)
		if r.Method == "HEAD" {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			return
		}
		var start, end int64
		if _, e := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); e != nil {
			w.WriteHeader(400)
			return
		}
		if start < 0 || end >= int64(len(data)) || end < start {
			w.WriteHeader(416)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(206)
		for offset := start; offset <= end; {
			select {
			case <-r.Context().Done():
				return
			default:
			}
			n := min(int64(32<<10), end-offset+1)
			if _, e := w.Write(data[offset : offset+n]); e != nil {
				return
			}
			offset += n
			time.Sleep(time.Millisecond)
		}
	}))
	defer server.Close()
	state := filepath.Join(t.TempDir(), "state")
	first := helperDaemon(t, state)
	alive := true
	t.Cleanup(func() {
		if alive {
			first.Process.Kill()
			first.Wait()
		}
	})
	var task model.Task
	_ = json.Unmarshal(cli(t, state, "task", "add", "--json", server.URL+"/recover.bin"), &task)
	cli(t, state, "task", "wait", "--id", task.ID, "--condition", "metadata", "--timeout", "5")
	cli(t, state, "task", "policy-set", "--id", task.ID, "--connections", "1")
	cli(t, state, "task", "start", "--id", task.ID)
	checkpoint := filepath.Join(state, "downloads", task.ID, "checkpoint.json")
	deadline := time.Now().Add(10 * time.Second)
	found := false
	for time.Now().Before(deadline) {
		b, e := os.ReadFile(checkpoint)
		if e == nil {
			var c engine.Checkpoint
			if json.Unmarshal(b, &c) == nil && len(c.Blocks) > 0 && !c.Complete {
				found = true
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !found {
		t.Fatal("no durable checkpoint before crash")
	}
	if e := first.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	_ = first.Wait()
	alive = false
	second := helperDaemon(t, state)
	t.Cleanup(func() { second.Process.Kill(); second.Wait() })
	var result struct {
		Task    model.Task `json:"task"`
		Matched bool       `json:"matched"`
	}
	if e := json.Unmarshal(cli(t, state, "task", "wait", "--json", "--id", task.ID, "--timeout", "10"), &result); e != nil {
		t.Fatal(e)
	}
	if !result.Matched {
		t.Fatal("crashed task was not recovered", result.Task.Issue)
	}
	got, e := os.ReadFile(result.Task.Metadata.Files[0].Path)
	if e != nil || !bytes.Equal(got, data) {
		t.Fatal("recovered file is corrupt", e)
	}
}

func TestRemoteDaemonProcessWorkflow(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "server-state")
	// Reuse a short-lived local TLS server's test certificate. The child daemon
	// loads these PEM files through the production --tls-cert/--tls-key path.
	fixture := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	pair := fixture.TLS.Certificates[0]
	certPath := filepath.Join(root, "cert.pem")
	keyPath := filepath.Join(root, "key.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: pair.Certificate[0]}), 0600); err != nil {
		t.Fatal(err)
	}
	encoded, err := x509.MarshalPKCS8PrivateKey(pair.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded}), 0600); err != nil {
		t.Fatal(err)
	}
	fixture.Close()
	tokenPath := filepath.Join(root, "token")
	if err := os.WriteFile(tokenPath, []byte(strings.Repeat("a", 64)), 0600); err != nil {
		t.Fatal(err)
	}
	flags := []string{"--listen", "127.0.0.1:0", "--token-file", tokenPath, "--tls-cert", certPath, "--tls-key", keyPath}
	raw, _ := json.Marshal(flags)
	logPath := filepath.Join(root, "daemon.log")
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperDaemon$")
	cmd.Env = append(os.Environ(), "FETCHRA_TEST_HELPER=1", "FETCHRA_TEST_STATE="+state, "FETCHRA_TEST_ARGS="+string(raw))
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { cmd.Process.Kill(); cmd.Wait() }()
	waitSocket(t, state)
	logs, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := ""
	for _, line := range strings.Split(string(logs), "\n") {
		if strings.HasPrefix(line, "fetchra remote endpoint ") {
			endpoint = strings.TrimPrefix(line, "fetchra remote endpoint ")
		}
	}
	if endpoint == "" {
		t.Fatal("daemon did not report its remote endpoint", string(logs))
	}
	payload := []byte("real process remote download")
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "process.bin", time.Time{}, bytes.NewReader(payload))
	}))
	defer origin.Close()
	clientState := filepath.Join(root, "client-state")
	invoke := func(args ...string) json.RawMessage {
		t.Helper()
		var out bytes.Buffer
		global := []string{"--state", clientState, "--endpoint", endpoint, "--token-file", tokenPath, "--ca-file", certPath}
		if err := run(context.Background(), append(global, args...), &out, io.Discard); err != nil {
			t.Fatal(args, err, out.String())
		}
		var envelope struct {
			Result json.RawMessage `json:"result"`
		}
		if err := json.Unmarshal(out.Bytes(), &envelope); err != nil {
			t.Fatal(err)
		}
		return envelope.Result
	}
	var task model.Task
	if err := json.Unmarshal(invoke("task", "add", "--start", "--request-key", "process", origin.URL+"/process.bin"), &task); err != nil {
		t.Fatal(err)
	}
	var waited struct {
		Matched bool `json:"matched"`
	}
	json.Unmarshal(invoke("task", "wait", "--id", task.ID, "--timeout", "10"), &waited)
	if !waited.Matched {
		t.Fatal("child daemon did not complete task")
	}
	var file model.File
	json.Unmarshal(invoke("task", "fetch", "--id", task.ID), &file)
	data, err := os.ReadFile(file.Path)
	if err != nil || !bytes.Equal(data, payload) || file.Integrity != "sha256_verified" {
		t.Fatal("remote process output mismatch", err, file)
	}
}
func TestHelpAndVersionExitSuccessfully(t *testing.T) {
	for _, args := range [][]string{{"--help"}, {"task", "add", "--help"}, {"serve", "--help"}, {"--version"}} {
		var out bytes.Buffer
		if err := run(context.Background(), args, &out, io.Discard); err != nil {
			t.Fatal(args, err)
		}
		if len(args) == 1 && args[0] == "--version" && strings.TrimSpace(out.String()) != model.Version {
			t.Fatal("incorrect version")
		}
	}
}

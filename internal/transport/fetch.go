package transport

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/RyanChen-cn/fetchra/internal/engine"
	"github.com/RyanChen-cn/fetchra/internal/model"
	"golang.org/x/time/rate"
)

// Fetch uses the same verified checkpoint and publication code as URL downloads.
// The local lock prevents concurrent CLI/MCP sessions from writing one checkpoint.
func (c *Client) Fetch(ctx context.Context, id string, index int, root string) (model.File, error) {
	if id == "" || strings.ContainsAny(id, "/\\\x00") || index < 0 {
		return model.File{}, model.Err("INVALID_ARGUMENT", "valid task_id and file_index are required")
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%d", c.endpoint, id, index)))
	key := hex.EncodeToString(sum[:12])
	if err := engine.PrepareDir(root); err != nil {
		return model.File{}, err
	}
	lockDir := filepath.Join(root, ".locks")
	if err := engine.PrepareDir(lockDir); err != nil {
		return model.File{}, err
	}
	lockRoot, err := os.OpenRoot(lockDir)
	if err != nil {
		return model.File{}, err
	}
	lock, err := lockRoot.OpenFile(key+".lock", os.O_CREATE|os.O_RDWR, 0600)
	lockRoot.Close()
	if err != nil {
		return model.File{}, err
	}
	defer lock.Close()
	for {
		err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
			return model.File{}, model.Err("DISK_ERROR", "cannot lock local transfer")
		}
		select {
		case <-ctx.Done():
			return model.File{}, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	h := &engine.HTTP{NoRedirect: true, Transport: fileTransport{c.HTTP.Transport}, Limit: rate.NewLimiter(rate.Inf, 64<<10), Hosts: c.hosts, Credentials: map[string]model.Credential{"node": {Headers: map[string]string{"Authorization": "Bearer " + c.token}}}}
	source := model.Source{Protocol: "http", URL: fmt.Sprintf("%s/files/%s/%d", c.endpoint, id, index), CredentialRef: "node"}
	// Unix clients use their socket transport as well; a local copy is supported.
	if c.token == "" {
		source.CredentialRef = ""
	}
	metadata, err := h.Probe(ctx, source)
	if err != nil {
		return model.File{}, err
	}
	expected := strings.TrimSuffix(strings.TrimPrefix(metadata.ETag, `"sha256-`), `"`)
	decoded, err := hex.DecodeString(expected)
	if err != nil || len(decoded) != 32 {
		return model.File{}, model.Err("PROTOCOL_ERROR", "node file endpoint did not supply a SHA-256 validator")
	}
	task := model.Task{ID: key, Source: source, Metadata: metadata, Dir: filepath.Join(root, key), SHA256: expected, Policy: model.TaskPolicy{Priority: "normal", Connections: 4}}
	var result model.File
	err = engine.RetryDownload(ctx, h, task, func(p engine.Progress) {
		if len(p.Files) > 0 && p.Files[0].Ready {
			result = p.Files[0]
		}
	})
	if err != nil {
		return model.File{}, err
	}
	return result.Public(), nil
}

// Preserve stable domain errors from the file endpoint through the common HTTP
// engine, instead of collapsing FILE_NOT_READY into a generic HTTP status.
type fileTransport struct{ base http.RoundTripper }

func (t fileTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	if response.StatusCode >= 400 && strings.HasPrefix(response.Header.Get("Content-Type"), "application/json") {
		var envelope Envelope
		err = json.NewDecoder(io.LimitReader(response.Body, 16<<10)).Decode(&envelope)
		response.Body.Close()
		if err == nil && envelope.Error != nil {
			return nil, envelope.Error
		}
		return nil, model.Err("PROTOCOL_ERROR", "invalid node file response")
	}
	return response, nil
}

// Package control routes client tools to independent download daemons. It owns
// no transfers: losing this process never changes a remote task's desired state.
package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/RyanChen-cn/fetchra/internal/model"
	"github.com/RyanChen-cn/fetchra/internal/transport"
	bolt "go.etcd.io/bbolt"
)

type Config struct{ Socket, Endpoint, TokenFile, CAFile, NodesFile, Node, StateDir, FetchDir string }
type NodeConfig struct {
	Name      string `json:"name"`
	Endpoint  string `json:"endpoint"`
	TokenFile string `json:"token_file"`
	CAFile    string `json:"ca_file,omitempty"`
}
type NodeFile struct {
	Default string       `json:"default,omitempty"`
	Nodes   []NodeConfig `json:"nodes"`
}
type Controller struct {
	nodes                      map[string]*transport.Client
	endpoints                  map[string]string
	selected, fetchDir, routes string
	qualify                    bool
}

func New(cfg Config) (*Controller, error) {
	if cfg.FetchDir == "" {
		cfg.FetchDir = filepath.Join(cfg.StateDir, "fetched")
	}
	abs, err := filepath.Abs(cfg.FetchDir)
	if err != nil {
		return nil, err
	}
	cfg.FetchDir = abs
	c := &Controller{nodes: map[string]*transport.Client{}, endpoints: map[string]string{}, fetchDir: cfg.FetchDir, routes: filepath.Join(cfg.StateDir, "node-routes.db")}
	if cfg.Endpoint != "" && cfg.NodesFile != "" {
		return nil, model.Err("INVALID_CONFIG", "choose endpoint or nodes configuration")
	}
	if cfg.NodesFile != "" {
		data, err := os.ReadFile(cfg.NodesFile)
		if err != nil {
			return nil, model.Err("INVALID_CONFIG", "cannot read nodes configuration")
		}
		var file NodeFile
		decoder := json.NewDecoder(strings.NewReader(string(data)))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&file) != nil || decoder.Decode(new(any)) != io.EOF || len(file.Nodes) == 0 || len(file.Nodes) > 32 {
			return nil, model.Err("INVALID_CONFIG", "nodes configuration must contain 1..32 unique nodes")
		}
		c.qualify = true
		c.selected = file.Default
		for _, node := range file.Nodes {
			if !validName(node.Name) || c.nodes[node.Name] != nil {
				c.Close()
				return nil, model.Err("INVALID_CONFIG", "node names must be unique letters, digits, underscores, or hyphens; auto is reserved")
			}
			base := filepath.Dir(cfg.NodesFile)
			if !filepath.IsAbs(node.TokenFile) {
				node.TokenFile = filepath.Join(base, node.TokenFile)
			}
			if node.CAFile != "" && !filepath.IsAbs(node.CAFile) {
				node.CAFile = filepath.Join(base, node.CAFile)
			}
			client, err := remote(node.Endpoint, node.TokenFile, node.CAFile)
			if err != nil {
				c.Close()
				return nil, err
			}
			c.nodes[node.Name] = client
			c.endpoints[node.Name] = node.Endpoint
		}
		if c.selected == "" {
			c.selected = "auto"
		}
	} else if cfg.Endpoint != "" {
		client, err := remote(cfg.Endpoint, cfg.TokenFile, cfg.CAFile)
		if err != nil {
			return nil, err
		}
		c.nodes["remote"] = client
		c.endpoints["remote"] = cfg.Endpoint
		c.selected = "remote"
	} else {
		if cfg.TokenFile != "" || cfg.CAFile != "" {
			return nil, model.Err("INVALID_CONFIG", "token-file and ca-file require endpoint")
		}
		c.nodes["local"] = transport.NewClient(cfg.Socket)
		c.selected = "local"
	}
	if cfg.Node != "" {
		c.selected = cfg.Node
	}
	if c.selected != "auto" && c.nodes[c.selected] == nil {
		c.Close()
		return nil, model.Err("NODE_NOT_FOUND", "selected node is not configured")
	}
	if c.selected == "auto" && !c.qualify {
		c.Close()
		return nil, model.Err("INVALID_CONFIG", "auto routing requires nodes configuration")
	}
	return c, nil
}
func validName(name string) bool {
	if name == "" || name == "auto" {
		return false
	}
	for _, r := range name {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}
func remote(endpoint, tokenFile, caFile string) (*transport.Client, error) {
	token, err := transport.ReadToken(tokenFile)
	if err != nil {
		return nil, err
	}
	return transport.NewRemoteClient(transport.RemoteClientConfig{Endpoint: endpoint, Token: token, CAFile: caFile})
}
func (c *Controller) Close() {
	for _, node := range c.nodes {
		node.Close()
	}
}
func (c *Controller) Call(ctx context.Context, method string, raw json.RawMessage) (any, error) {

	if len(raw) == 0 || (strings.TrimSpace(string(raw)) == "null" && (method == "node_list" || method == "policy_get" || method == "service_status")) {
		raw = []byte("{}")
	}
	args := map[string]json.RawMessage{}
	if json.Unmarshal(raw, &args) != nil || args == nil {
		return nil, model.Err("INVALID_ARGUMENT", "arguments must be a JSON object")
	}
	if method == "node_list" {
		if len(args) > 0 {
			return nil, model.Err("INVALID_ARGUMENT", "node_list accepts no arguments")
		}
		return c.health(ctx), nil
	}
	node := c.selected
	if data := args["node"]; data != nil {
		if json.Unmarshal(data, &node) != nil {
			return nil, model.Err("INVALID_ARGUMENT", "node must be a string")
		}
		delete(args, "node")
	}
	var id string
	if data := args["task_id"]; data != nil {
		if json.Unmarshal(data, &id) != nil {
			return nil, model.Err("INVALID_ARGUMENT", "task_id must be a string")
		}
		if c.qualify {
			parts := strings.SplitN(id, ":", 2)
			if len(parts) == 2 {
				if explicit := argsNode(raw); explicit != "" && explicit != "auto" && explicit != parts[0] {
					return nil, model.Err("INVALID_ARGUMENT", "node conflicts with qualified task_id")
				}
				node, id = parts[0], parts[1]
				args["task_id"], _ = json.Marshal(id)
			}
		}
	}
	if method == "task_add" {
		encoded, _ := json.Marshal(args)
		var add model.Add
		decoder := json.NewDecoder(strings.NewReader(string(encoded)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&add); err != nil {
			return nil, model.Err("INVALID_ARGUMENT", "invalid task_add arguments")
		}
		if len(add.TorrentData) == 0 {
			source, err := model.ParseSource(add.Source)
			if err != nil {
				return nil, err
			}
			if source.Protocol == "torrent" && source.TorrentPath != "" {
				f, err := os.Open(source.TorrentPath)
				if err != nil {
					return nil, model.Err("INVALID_TORRENT", "cannot read local torrent file; use a magnet or upload torrent_data")
				}
				info, statErr := f.Stat()
				if statErr != nil || !info.Mode().IsRegular() {
					f.Close()
					return nil, model.Err("INVALID_TORRENT", "torrent source must be a regular file")
				}
				add.TorrentData, err = io.ReadAll(io.LimitReader(f, model.MaxTorrentSize+1))
				f.Close()
				if err != nil || len(add.TorrentData) > model.MaxTorrentSize {
					return nil, model.Err("INVALID_TORRENT", "torrent upload exceeds 8 MiB")
				}
				add.Source = filepath.Base(source.TorrentPath)
			}
		}
		encoded, _ = json.Marshal(add)
		if c.qualify {
			selected, err := c.assign(ctx, node, add, encoded)
			if err != nil {
				return nil, err
			}
			node = selected
		}
		args = map[string]json.RawMessage{}
		_ = json.Unmarshal(encoded, &args)
	}
	client := c.nodes[node]
	if client == nil {
		if node == "auto" {
			return nil, model.Err("NODE_REQUIRED", "choose node for list, policy, and status calls, or use a qualified task_id")
		}
		return nil, model.Err("NODE_NOT_FOUND", "node is not configured")
	}
	if method == "task_fetch" {
		for key := range args {
			if key != "task_id" && key != "file_index" {
				return nil, model.Err("INVALID_ARGUMENT", "task_fetch accepts task_id, file_index, and optional node")
			}
		}
		index := 0
		if data := args["file_index"]; data != nil {
			if json.Unmarshal(data, &index) != nil {
				return nil, model.Err("INVALID_ARGUMENT", "file_index must be an integer")
			}
		}
		return client.Fetch(ctx, id, index, c.fetchDir)
	}
	encoded, _ := json.Marshal(args)
	result, err := client.Call(ctx, method, encoded)
	if err != nil {
		return nil, err
	}
	if c.qualify {
		qualify(result, node)
	}
	return result, nil
}
func argsNode(raw json.RawMessage) string {
	var a struct {
		Node string `json:"node"`
	}
	_ = json.Unmarshal(raw, &a)
	return a.Node
}
func qualify(value any, node string) {
	switch v := value.(type) {
	case map[string]any:
		for k, item := range v {
			if k == "task_id" || k == "removed" {
				if id, ok := item.(string); ok && id != "" {
					v[k] = node + ":" + id
					v["node"] = node
				}
			}
			qualify(item, node)
		}
	case []any:
		for _, item := range v {
			qualify(item, node)
		}
	}
}

type Health struct {
	Name     string       `json:"name"`
	Endpoint string       `json:"endpoint,omitempty"`
	Status   any          `json:"status,omitempty"`
	Error    *model.Error `json:"error,omitempty"`
}

func (c *Controller) health(ctx context.Context) []Health {
	names := make([]string, 0, len(c.nodes))
	for name := range c.nodes {
		names = append(names, name)
	}
	sort.Strings(names)
	results := make([]Health, len(names))
	var wg sync.WaitGroup
	for i, name := range names {
		wg.Add(1)
		go func(i int, name string) {
			defer wg.Done()
			probe, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			status, err := c.nodes[name].Call(probe, "service_status", nil)
			h := Health{Name: name, Endpoint: c.endpoints[name], Status: status}
			if err != nil {
				h.Error = model.AsError(err)
			}
			results[i] = h
		}(i, name)
	}
	wg.Wait()
	return results
}

type route struct {
	Node     string `json:"node"`
	Endpoint string `json:"endpoint"`
	Digest   string `json:"digest"`
}

func (c *Controller) route(key string, candidate *route) (*route, error) {
	if key == "" {
		return candidate, nil
	}
	if err := os.MkdirAll(filepath.Dir(c.routes), 0700); err != nil {
		return nil, model.Err("DISK_ERROR", "cannot create routing state")
	}
	db, err := bolt.Open(c.routes, 0600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, model.Err("DISK_ERROR", "cannot open routing state")
	}
	defer db.Close()
	var existing *route
	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("routes"))
		if err != nil {
			return err
		}
		if data := b.Get([]byte(key)); data != nil {
			existing = &route{}
			return json.Unmarshal(data, existing)
		}
		if candidate != nil {
			data, _ := json.Marshal(candidate)
			if err := b.Put([]byte(key), data); err != nil {
				return err
			}
			copy := *candidate
			existing = &copy
		}
		return nil
	})
	if err != nil {
		return nil, model.Err("DISK_ERROR", "cannot persist node assignment")
	}
	return existing, nil
}
func (c *Controller) assign(ctx context.Context, node string, add model.Add, encoded []byte) (string, error) {
	explicit := node != "auto"
	sum := sha256.Sum256(encoded)
	digest := hex.EncodeToString(sum[:])
	stored, err := c.route(add.RequestKey, nil)
	if err != nil {
		return "", err
	}
	if stored == nil {
		if node == "auto" {
			if add.RequestKey == "" {
				return "", model.Err("REQUEST_KEY_REQUIRED", "automatic node selection requires request_key for reliable retries")
			}
			health := c.health(ctx)
			best := 1e30
			bestTie := ""
			node = ""
			for _, h := range health {
				if h.Error != nil {
					continue
				}
				status, ok := h.Status.(map[string]any)
				if !ok {
					continue
				}
				policy, ok := status["policy"].(map[string]any)
				if !ok {
					continue
				}
				capacity := number(policy["active_downloads"])
				if capacity <= 0 {
					continue
				}
				pressure := (number(status["active_downloads"]) + number(status["queued_downloads"])) / capacity
				if !add.Start {
					probeCapacity := number(policy["active_probes"])
					if probeCapacity > 0 {
						pressure = (number(status["active_probes"]) + number(status["pending_probes"])) / probeCapacity
					}
				}
				tieSum := sha256.Sum256([]byte(add.RequestKey + "/" + h.Name))
				tie := hex.EncodeToString(tieSum[:])
				if pressure < best || (pressure == best && tie < bestTie) {
					best, bestTie = pressure, tie
					node = h.Name
				}
			}
			if node == "" {
				return "", &model.Error{Code: "NO_AVAILABLE_NODE", Message: "no configured node is reachable", Retryable: true}
			}
		}
		if c.nodes[node] == nil {
			return "", model.Err("NODE_NOT_FOUND", "node is not configured")
		}
		stored, err = c.route(add.RequestKey, &route{node, c.endpoints[node], digest})
		if err != nil {
			return "", err
		}
	}
	if stored.Digest != digest {
		return "", model.Err("REQUEST_CONFLICT", "request_key was used with different parameters")
	}
	if explicit && stored.Node != node {
		return "", model.Err("REQUEST_CONFLICT", "request_key is assigned to a different node")
	}
	if c.nodes[stored.Node] == nil || c.endpoints[stored.Node] != stored.Endpoint {
		return "", model.Err("NODE_CONFIG_CHANGED", "the assigned node was removed or its endpoint changed; restore it or use a new request_key")
	}
	return stored.Node, nil
}
func number(v any) float64 {
	switch n := v.(type) {
	case json.Number:
		f, _ := n.Float64()
		return f
	case float64:
		return n
	case int:
		return float64(n)
	}
	return 0
}

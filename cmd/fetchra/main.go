package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/RyanChen-cn/fetchra/internal/control"
	"github.com/RyanChen-cn/fetchra/internal/model"
	"github.com/RyanChen-cn/fetchra/internal/service"
	"github.com/RyanChen-cn/fetchra/internal/transport"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if e := run(ctx, os.Args[1:], os.Stdout, os.Stderr); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func defaultState() string {
	if p := os.Getenv("XDG_DATA_HOME"); p != "" {
		return filepath.Join(p, "fetchra")
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".local", "share", "fetchra")
}
func run(ctx context.Context, args []string, out, stderr io.Writer) error {
	global := flag.NewFlagSet("fetchra", flag.ContinueOnError)
	global.SetOutput(stderr)
	state := global.String("state", defaultState(), "state directory")
	socket := global.String("socket", "", "local Unix socket (default STATE/service.sock)")
	endpoint := global.String("endpoint", "", "remote HTTP/HTTPS node origin")
	tokenFile := global.String("token-file", "", "private remote access token file")
	caFile := global.String("ca-file", "", "additional trusted PEM CA for remote node")
	nodesFile := global.String("nodes", "", "optional named nodes JSON configuration")
	node := global.String("node", "", "named node or auto for task add")
	version := global.Bool("version", false, "print version")
	fetchDir := global.String("fetch-dir", "", "local retrieved file root (default STATE/fetched)")
	if e := global.Parse(args); e != nil {
		if e == flag.ErrHelp {
			return nil
		}
		return e
	}
	if *version {
		fmt.Fprintln(out, model.Version)
		return nil
	}
	args = global.Args()
	if len(args) == 0 {
		fmt.Fprintln(out, "fetchra [--state DIR] [--socket PATH] serve|mcp|task|policy|events|status|nodes\nRemote: --endpoint URL --token-file FILE [--ca-file FILE], or --nodes FILE [--node NAME]. Use fetchra task add --help for task options. Global flags precede the command.")
		return nil
	}
	if *socket == "" {
		*socket = filepath.Join(*state, "service.sock")
	}
	switch args[0] {
	case "serve":
		fs := flag.NewFlagSet("serve", flag.ContinueOnError)
		fs.SetOutput(stderr)
		downloads := fs.String("downloads", filepath.Join(*state, "downloads"), "download root")
		credentialFile := fs.String("credentials", "", "private JSON credential map")
		listen := fs.String("listen", "", "optional remote IP:port listener")
		serverToken := fs.String("token-file", *tokenFile, "private access token for remote listener")
		cert := fs.String("tls-cert", "", "PEM certificate for remote listener")
		privateKey := fs.String("tls-key", "", "PEM private key for remote listener")
		if e := fs.Parse(args[1:]); e != nil {
			if e == flag.ErrHelp {
				return nil
			}
			return e
		}
		credentials := map[string]model.Credential{}
		if *credentialFile != "" {
			info, e := os.Stat(*credentialFile)
			if e != nil {
				return e
			}
			if info.Mode().Perm()&0077 != 0 {
				return fmt.Errorf("credentials file must have permissions 0600")
			}
			b, e := os.ReadFile(*credentialFile)
			if e != nil {
				return e
			}
			if e = json.Unmarshal(b, &credentials); e != nil {
				return fmt.Errorf("invalid credential configuration")
			}
		}
		remote := transport.RemoteConfig{Listen: *listen, CertFile: *cert, KeyFile: *privateKey, OnListen: func(socket, endpoint string) {
			fmt.Fprintln(stderr, "fetchra service listening on", socket)
			if endpoint != "" {
				fmt.Fprintln(stderr, "fetchra remote endpoint", endpoint)
			}
		}}
		if *listen != "" {
			var err error
			remote.Token, err = transport.ReadToken(*serverToken)
			if err != nil {
				return err
			}
		}
		if *listen == "" && (*serverToken != "" || *cert != "" || *privateKey != "") {
			return model.Err("INVALID_CONFIG", "remote token and TLS flags require --listen")
		}
		s, e := service.New(*state, *downloads, credentials)
		if e != nil {
			return e
		}
		defer s.Close()
		return transport.ServeWithRemote(ctx, *socket, s, remote)
	}
	if *fetchDir == "" {
		*fetchDir = filepath.Join(*state, "fetched")
	}
	caller, err := control.New(control.Config{Socket: *socket, Endpoint: *endpoint, TokenFile: *tokenFile, CAFile: *caFile, NodesFile: *nodesFile, Node: *node, StateDir: *state, FetchDir: *fetchDir})
	if err != nil {
		return err
	}
	defer caller.Close()
	if args[0] == "mcp" {
		return transport.RunMCP(ctx, caller)
	}
	method := ""
	rest := []string{}
	switch args[0] {
	case "task", "policy":
		if len(args) < 2 {
			return fmt.Errorf("subcommand required")
		}
		method = args[0] + "_" + strings.ReplaceAll(args[1], "-", "_")
		rest = args[2:]
	case "events":
		method = "events_list"
		rest = args[1:]
	case "nodes":
		method = "node_list"
		rest = args[1:]
	case "status":
		method = "service_status"
		rest = args[1:]
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
	fs := flag.NewFlagSet(method, flag.ContinueOnError)
	fs.SetOutput(stderr)
	jsonOutput := fs.Bool("json", false, "print stable JSON envelope")
	raw := fs.String("params", "", "exact JSON RPC parameters (advanced)")
	taskID := fs.String("id", "", "task ID (NODE:ID in nodes mode)")
	fileIndex := fs.Int("file-index", 0, "completed file index to retrieve locally")
	revision := fs.Uint64("revision", 0, "expected task revision")
	start := fs.Bool("start", false, "start after probing; select all torrent files")
	key := fs.String("request-key", "", "idempotency key")
	sha := fs.String("sha256", "", "expected SHA-256")
	filename := fs.String("filename", "", "output basename")
	credential := fs.String("credential", "", "configured credential reference")
	files := fs.String("files", "", "comma-separated indices (INDEX:high for file priority)")
	priority := fs.String("priority", "normal", "task priority: high, normal, low")
	connections := fs.Int("connections", 4, "HTTP segment concurrency 1..4")
	seed := fs.Bool("seed", false, "keep seeding torrent in idle slots")
	filter := fs.String("state", "", "task state filter")
	offset := fs.Int("offset", 0, "page offset")
	limit := fs.Int("limit", 50, "page size 1..200")
	cursor := fs.Uint64("cursor", 0, "event cursor")
	condition := fs.String("condition", "completed", "wait condition: metadata, completed, files, idle")
	timeout := fs.Int("timeout", 20, "wait seconds 1..30")
	active := fs.Int("active", 3, "active download tasks")
	probes := fs.Int("probes", 2, "active probes")
	perHost := fs.Int("per-host", 4, "HTTP requests per origin")
	downloadRate := fs.Int64("download-rate", 0, "shared payload bytes/sec, 0 unlimited")
	uploadRate := fs.Int64("upload-rate", 0, "upload bytes/sec, 0 unlimited")
	if e := fs.Parse(rest); e != nil {
		if e == flag.ErrHelp {
			return nil
		}
		return e
	}
	var params any
	if method == "task_add" {
		if fs.NArg() != 1 && *raw == "" {
			return fmt.Errorf("task add requires one source; place flags before SOURCE")
		}
		source := ""
		if fs.NArg() > 0 {
			source = fs.Arg(0)
		}
		params = model.Add{Source: source, CredentialRef: *credential, RequestKey: *key, Start: *start, SHA256: *sha, Filename: *filename}
	} else {
		p := model.Params{TaskID: *taskID, State: *filter, Offset: *offset, Limit: *limit, Cursor: *cursor, Condition: *condition, TimeoutSeconds: *timeout}
		if *revision > 0 {
			p.ExpectedRevision = revision
		}
		if *files != "" {
			for _, entry := range strings.Split(*files, ",") {
				parts := strings.Split(entry, ":")
				n, e := strconv.Atoi(parts[0])
				if e != nil {
					return fmt.Errorf("invalid file index")
				}
				prio := "normal"
				if len(parts) > 1 {
					prio = parts[1]
				}
				p.Files = append(p.Files, model.Selection{Index: n, Priority: prio})
				p.FileIndices = append(p.FileIndices, n)
			}
		}
		if method == "task_policy_set" {
			p.TaskPolicy = &model.TaskPolicy{Priority: *priority, Connections: *connections, Seed: *seed}
		}
		if method == "policy_set" {
			p.ResourcePolicy = &model.Policy{Active: *active, Probes: *probes, PerHost: *perHost, DownloadRate: *downloadRate, UploadRate: *uploadRate}
		}
		params = p
	}
	if method == "node_list" || method == "service_status" || method == "policy_get" {
		params = map[string]any{}
	}
	if method == "task_fetch" {
		params = map[string]any{"task_id": *taskID, "file_index": *fileIndex}
	}
	b, e := json.Marshal(params)
	if e != nil {
		return e
	}
	if *raw != "" {
		b = []byte(*raw)
		if !json.Valid(b) {
			return fmt.Errorf("params must be JSON")
		}
	}
	result, e := caller.Call(ctx, method, b)
	envelope := transport.Envelope{Result: result}
	if e != nil {
		envelope = transport.Envelope{Error: model.AsError(e)}
	}
	encoder := json.NewEncoder(out)
	if !*jsonOutput {
		encoder.SetIndent("", "  ")
	}
	if writeErr := encoder.Encode(envelope); writeErr != nil {
		return writeErr
	}
	return e
}

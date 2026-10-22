package transport

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/RyanChen-cn/fetchra/internal/engine"
	"github.com/RyanChen-cn/fetchra/internal/model"
)

// Remote access is opt-in. Plain HTTP is limited to loopback (e.g. SSH tunnels).
type RemoteConfig struct {
	Listen, Token, CertFile, KeyFile string
	OnListen                         func(socket, endpoint string)
}
type RemoteClientConfig struct{ Endpoint, Token, CAFile string }

func ReadToken(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", model.Err("INVALID_CONFIG", "cannot read token file")
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 4096 {
		return "", model.Err("INVALID_CONFIG", "token file must be a private regular file (0600, at most 4096 bytes)")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", model.Err("INVALID_CONFIG", "cannot read token file")
	}
	token := strings.TrimSpace(string(data))
	if len(token) < 32 || strings.ContainsAny(token, " \t\r\n") {
		return "", model.Err("INVALID_CONFIG", "access token must contain at least 32 non-whitespace characters")
	}
	return token, nil
}
func NewRemoteClient(cfg RemoteClientConfig) (*Client, error) {
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, model.Err("INVALID_CONFIG", "endpoint must be an HTTP/HTTPS origin without credentials, query, or path")
	}
	if u.Scheme != "https" && (u.Scheme != "http" || !loopback(u.Hostname())) {
		return nil, model.Err("INVALID_CONFIG", "remote endpoints require HTTPS; HTTP is allowed only on loopback")
	}
	if len(cfg.Token) < 32 || strings.ContainsAny(cfg.Token, " \t\r\n") {
		return nil, model.Err("INVALID_CONFIG", "remote access requires a token with at least 32 characters")
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DisableCompression = true
	tr.TLSHandshakeTimeout = 10 * time.Second
	tr.ResponseHeaderTimeout = 40 * time.Second // task_wait may hold a response for 30 seconds.
	tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.CAFile != "" {
		data, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, model.Err("INVALID_CONFIG", "cannot read CA file")
		}
		roots, err := x509.SystemCertPool()
		if err != nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(data) {
			return nil, model.Err("INVALID_CONFIG", "CA file contains no certificates")
		}
		tr.TLSClientConfig.RootCAs = roots
	}
	return &Client{HTTP: &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error {
		return model.Err("RPC_ERROR", "node redirects are not allowed")
	}}, endpoint: strings.TrimSuffix(cfg.Endpoint, "/"), token: cfg.Token, hosts: engine.NewHosts(4)}, nil
}
func loopback(host string) bool {
	ip := net.ParseIP(host)
	return host == "localhost" || (ip != nil && ip.IsLoopback())
}
func AuthenticatedHandler(token string, next http.Handler) http.Handler {
	expected := sha256.Sum256([]byte("Bearer " + token))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		actual := sha256.Sum256([]byte(r.Header.Get("Authorization")))
		if len(token) < 32 || subtle.ConstantTimeCompare(actual[:], expected[:]) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="fetchra"`)
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return
		}
		// Browser cross-origin requests do not need access to this operator API.
		if r.Header.Get("Origin") != "" {
			http.Error(w, "browser origins are not supported", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type FileOpener interface {
	OpenFile(context.Context, string, int) (*os.File, model.File, error)
}

func NodeHandler(c Caller) http.Handler {
	rpc := Handler(c)
	exports := make(chan struct{}, 8)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if !strings.HasPrefix(r.URL.Path, "/files/") {
			rpc.ServeHTTP(w, r)
			return
		}
		if r.Method != "GET" && r.Method != "HEAD" {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", 405)
			return
		}
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/files/"), "/")
		if len(parts) != 2 || parts[0] == "" || strings.Contains(r.Header.Get("Range"), ",") {
			http.Error(w, "invalid file request", 400)
			return
		}
		index, err := strconv.Atoi(parts[1])
		opener, ok := c.(FileOpener)
		if err != nil || index < 0 || !ok {
			http.NotFound(w, r)
			return
		}
		select {
		case exports <- struct{}{}:
			defer func() { <-exports }()
		case <-r.Context().Done():
			return
		}
		f, file, err := opener.OpenFile(r.Context(), parts[0], index)
		if err != nil {
			code := model.AsError(err).Code
			status := 409
			if code == "TASK_NOT_FOUND" || code == "INVALID_ARGUMENT" {
				status = 404
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(Envelope{Error: model.AsError(err)})
			return
		}
		defer f.Close()
		info, err := f.Stat()
		if err != nil {
			http.Error(w, "DISK_ERROR", 500)
			return
		}
		w.Header().Set("ETag", `"sha256-`+file.LocalSHA256+`"`)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": filepath.Base(file.Name)}))
		var content io.ReadSeeker = f
		if limiter, ok := c.(interface {
			ExportReader(context.Context, io.ReadSeeker) io.ReadSeeker
		}); ok {
			content = limiter.ExportReader(r.Context(), f)
		}
		http.ServeContent(exportWriter{w}, r, filepath.Base(file.Name), info.ModTime(), content)
	})
}

func unixListener(socket string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(socket), 0700); err != nil {
		return nil, err
	}
	if info, err := os.Lstat(socket); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, model.Err("INVALID_PATH", "socket path is occupied by a non-socket")
		}
		conn, dialErr := net.DialTimeout("unix", socket, time.Second)
		if dialErr == nil {
			conn.Close()
			return nil, model.Err("SERVICE_RUNNING", "another service is listening")
		}
		if err = os.Remove(socket); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		return nil, err
	}
	if err = os.Chmod(socket, 0600); err != nil {
		listener.Close()
		os.Remove(socket)
		return nil, err
	}
	return listener, nil
}

// Bind all listeners before starting either server; configuration errors cannot
// leave a partially started daemon behind.
func ServeWithRemote(ctx context.Context, socket string, c Caller, cfg RemoteConfig) error {
	var remote net.Listener
	if cfg.Listen != "" {
		host, _, err := net.SplitHostPort(cfg.Listen)
		if err != nil {
			return model.Err("INVALID_CONFIG", "listen must be IP:port")
		}
		if len(cfg.Token) < 32 || strings.ContainsAny(cfg.Token, " \t\r\n") {
			return model.Err("INVALID_CONFIG", "remote listener requires a private access token")
		}
		var cert *tls.Certificate
		if cfg.CertFile != "" || cfg.KeyFile != "" {
			pair, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
			if err != nil {
				return model.Err("INVALID_CONFIG", "cannot load TLS certificate and key")
			}
			cert = &pair
		} else if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
			return model.Err("INVALID_CONFIG", "non-loopback listeners require TLS certificate and key")
		}
		remote, err = net.Listen("tcp", cfg.Listen)
		if err != nil {
			return err
		}
		if cert != nil {
			remote = tls.NewListener(remote, &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{*cert}})
		}
		defer remote.Close()
	}
	local, err := unixListener(socket)
	if err != nil {
		return err
	}
	defer local.Close()
	defer os.Remove(socket)
	server := func(handler http.Handler) *http.Server {
		return &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	}
	servers := []*http.Server{server(NodeHandler(c))}
	listeners := []net.Listener{local}
	if remote != nil {
		servers = append(servers, server(AuthenticatedHandler(cfg.Token, NodeHandler(c))))
		listeners = append(listeners, remote)
	}
	if cfg.OnListen != nil {
		endpoint := ""
		if remote != nil {
			scheme := "http"
			if cfg.CertFile != "" {
				scheme = "https"
			}
			endpoint = scheme + "://" + remote.Addr().String()
		}
		cfg.OnListen(socket, endpoint)
	}
	results := make(chan error, len(servers))
	for i, s := range servers {
		go func(s *http.Server, l net.Listener) { results <- s.Serve(l) }(s, listeners[i])
	}
	select {
	case <-ctx.Done():
	case err = <-results:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
	}
	// Closing also cancels long exports and waits. Task workers belong to Service.
	for _, s := range servers {
		_ = s.Close()
	}
	return err
}

// Explicit flushing keeps headers and slow, rate-limited payloads observable.
// Each write is already bounded by the shared payload reader.
type exportWriter struct{ http.ResponseWriter }

func (w exportWriter) WriteHeader(status int) {
	w.ResponseWriter.WriteHeader(status)
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}
func (w exportWriter) Write(data []byte) (int, error) {
	n, err := w.ResponseWriter.Write(data)
	if err == nil {
		err = http.NewResponseController(w.ResponseWriter).Flush()
	}
	return n, err
}

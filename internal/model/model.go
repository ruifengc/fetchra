// Package model defines the protocol-independent public task model.
package model

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

type Error struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

func (e *Error) Error() string        { return e.Code + ": " + e.Message }
func Err(code, message string) *Error { return &Error{Code: code, Message: message} }
func AsError(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	if errors.Is(err, context.Canceled) {
		return Err("CANCELLED", "request was cancelled")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &Error{Code: "DEADLINE_EXCEEDED", Message: "request deadline exceeded", Retryable: true}
	}
	// Network errors can contain passwords and signed query parameters.
	return Err("INTERNAL", "operation failed; see service diagnostics")
}

const Version = "0.2.0"

const MaxTorrentSize = 8 << 20

type Source struct {
	TorrentData   []byte `json:"torrent_data,omitempty"`
	Protocol      string `json:"protocol"`
	URL           string `json:"url,omitempty"`
	TorrentPath   string `json:"torrent_path,omitempty"`
	CredentialRef string `json:"credential_ref,omitempty"`
}

func (s Source) Public() Source {
	s.TorrentData = nil
	if u, err := url.Parse(s.URL); err == nil {
		u.User = nil
		if u.Scheme != "magnet" {
			u.RawQuery = ""
			u.Fragment = ""
		} else {
			q := url.Values{}
			for _, v := range u.Query()["xt"] {
				q.Add("xt", v)
			}
			u.RawQuery = q.Encode()
		}
		s.URL = u.String()
	}
	return s
}

type Capabilities struct {
	Resume           bool   `json:"resume"`
	ResumeConfidence string `json:"resume_confidence"`
	Parallel         bool   `json:"parallel"`
	FileSelection    bool   `json:"file_selection"`
	Reason           string `json:"reason,omitempty"`
}
type File struct {
	VerifiedModTime int64  `json:"verified_mod_time_ns,omitempty"`
	Index           int    `json:"index"`
	Name            string `json:"name"`
	Size            *int64 `json:"size"`
	Selected        bool   `json:"selected"`
	Priority        string `json:"priority"`
	Completed       int64  `json:"completed_bytes"`
	Ready           bool   `json:"ready"`
	Path            string `json:"path,omitempty"`
	Integrity       string `json:"integrity,omitempty"`
	LocalSHA256     string `json:"local_sha256,omitempty"`
}

func (f File) Public() File { f.VerifiedModTime = 0; return f }

type Metadata struct {
	FileCount     int          `json:"file_count"`
	SelectedCount int          `json:"selected_count"`
	ReadyCount    int          `json:"ready_count"`
	InfoHash      string       `json:"info_hash,omitempty"`
	Files         []File       `json:"files"`
	Capabilities  Capabilities `json:"capabilities"`
	Size          *int64       `json:"size"`
	ETag          string       `json:"etag,omitempty"`
	Modified      string       `json:"modified,omitempty"`
	FinalURL      string       `json:"final_url,omitempty"`
	TorrentData   []byte       `json:"torrent_data,omitempty"`
}
type TaskPolicy struct {
	Priority    string `json:"priority"`
	Connections int    `json:"connections"`
	Seed        bool   `json:"seed"`
}
type Policy struct {
	Active       int   `json:"active_downloads"`
	Probes       int   `json:"active_probes"`
	PerHost      int   `json:"http_per_host"`
	DownloadRate int64 `json:"download_rate"`
	UploadRate   int64 `json:"upload_rate"`
}

func DefaultPolicy() Policy { return Policy{Active: 3, Probes: 2, PerHost: 4} }
func (p Policy) Validate() error {
	if p.Active < 1 || p.Active > 64 || p.Probes < 1 || p.Probes > 16 || p.PerHost < 1 || p.PerHost > 16 || p.DownloadRate < 0 || p.UploadRate < 0 {
		return Err("INVALID_ARGUMENT", "invalid resource policy (downloads 1..64, probes 1..16, per-host 1..16, rates >= 0)")
	}
	return nil
}

type Task struct {
	ID          string     `json:"task_id"`
	Revision    uint64     `json:"revision"`
	Source      Source     `json:"source"`
	State       string     `json:"state"`
	Phase       string     `json:"phase,omitempty"`
	Desired     string     `json:"desired_state"`
	Metadata    *Metadata  `json:"metadata,omitempty"`
	Policy      TaskPolicy `json:"policy"`
	SHA256      string     `json:"expected_sha256,omitempty"`
	Filename    string     `json:"filename,omitempty"`
	StartAll    bool       `json:"start_all"`
	Seeding     bool       `json:"seeding"`
	ObservedAt  time.Time  `json:"observed_at"`
	CreatedAt   time.Time  `json:"created_at"`
	QueuedAt    time.Time  `json:"queued_at"`
	Transferred int64      `json:"transferred_bytes"`
	Rate        float64    `json:"download_rate"`
	ETA         *float64   `json:"eta_seconds"`
	Issue       *Error     `json:"issue,omitempty"`
	Dir         string     `json:"-"`
}

func (t Task) Copy() Task {
	c := t
	c.Source.TorrentData = append([]byte(nil), t.Source.TorrentData...)
	if t.Metadata != nil {
		m := *t.Metadata
		m.TorrentData = append([]byte(nil), t.Metadata.TorrentData...)
		m.Files = append([]File(nil), t.Metadata.Files...)
		for i := range m.Files {
			if size := m.Files[i].Size; size != nil {
				n := *size
				m.Files[i].Size = &n
			}
		}
		if m.Size != nil {
			n := *m.Size
			m.Size = &n
		}
		c.Metadata = &m
	}
	if c.ETA != nil {
		eta := *c.ETA
		c.ETA = &eta
	}
	if c.Issue != nil {
		issue := *c.Issue
		c.Issue = &issue
	}
	return c
}

func (t Task) Public() Task {
	c := t
	c.Source = c.Source.Public()
	if c.Metadata != nil {
		m := *t.Metadata
		c.Metadata = &m
		m.FileCount = len(t.Metadata.Files)
		m.SelectedCount = 0
		m.ReadyCount = 0
		for _, f := range t.Metadata.Files {
			if f.Selected {
				m.SelectedCount++
			}
			if f.Ready {
				m.ReadyCount++
			}
		}
		m.Files = append([]File(nil), t.Metadata.Files[:min(len(t.Metadata.Files), 10)]...)
		for i := range m.Files {
			m.Files[i] = m.Files[i].Public()
		}
		c.Metadata.TorrentData = nil
		c.Metadata.FinalURL = ""
	}
	return c
}

type Selection struct {
	Index    int    `json:"index"`
	Priority string `json:"priority"`
}
type Add struct {
	TorrentData   []byte `json:"torrent_data,omitempty"`
	Source        string `json:"source"`
	CredentialRef string `json:"credential_ref,omitempty"`
	RequestKey    string `json:"request_key,omitempty"`
	Start         bool   `json:"start"`
	SHA256        string `json:"sha256,omitempty"`
	Filename      string `json:"filename,omitempty"`
}
type Params struct {
	TaskID           string      `json:"task_id,omitempty"`
	ExpectedRevision *uint64     `json:"expected_revision,omitempty"`
	Files            []Selection `json:"files,omitempty"`
	TaskPolicy       *TaskPolicy `json:"policy,omitempty"`
	ResourcePolicy   *Policy     `json:"resource_policy,omitempty"`
	State            string      `json:"state,omitempty"`
	Offset           int         `json:"offset,omitempty"`
	Limit            int         `json:"limit,omitempty"`
	Cursor           uint64      `json:"cursor,omitempty"`
	Condition        string      `json:"condition,omitempty"`
	FileIndices      []int       `json:"file_indices,omitempty"`
	TimeoutSeconds   int         `json:"timeout_seconds,omitempty"`
}
type Event struct {
	Cursor uint64    `json:"cursor"`
	TaskID string    `json:"task_id"`
	Kind   string    `json:"kind"`
	State  string    `json:"state"`
	At     time.Time `json:"at"`
}
type Credential struct {
	Username string            `json:"username,omitempty"`
	Password string            `json:"password,omitempty"`
	Headers  map[string]string `json:"headers,omitempty"`
}

func PriorityRank(s string) int {
	switch s {
	case "high":
		return 0
	case "normal":
		return 1
	case "low":
		return 2
	}
	return 3
}
func ParseSource(raw string) (Source, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return Source{}, Err("INVALID_ARGUMENT", "invalid source")
	}
	if u.User != nil {
		return Source{}, Err("INVALID_ARGUMENT", "use credential_ref instead of credentials in URLs")
	}
	s := Source{URL: raw}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
		s.Protocol = "http"
	case "ftp":
		s.Protocol = "ftp"
	case "magnet":
		s.Protocol = "torrent"
	case "":
		s.Protocol = "torrent"
		s.TorrentPath = raw
		s.URL = ""
	default:
		return Source{}, Err("UNSUPPORTED_PROTOCOL", fmt.Sprintf("unsupported scheme %q", u.Scheme))
	}
	if s.Protocol != "torrent" && u.Hostname() == "" {
		return Source{}, Err("INVALID_ARGUMENT", "source requires a host")
	}
	return s, nil
}

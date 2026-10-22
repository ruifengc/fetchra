package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/RyanChen-cn/fetchra/internal/engine"
	"github.com/RyanChen-cn/fetchra/internal/model"
)

type Caller interface {
	Call(context.Context, string, json.RawMessage) (any, error)
}
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}
type RPCError struct {
	Code    int          `json:"code"`
	Message string       `json:"message"`
	Data    *model.Error `json:"data,omitempty"`
}
type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
}
type Envelope struct {
	Result any          `json:"result,omitempty"`
	Error  *model.Error `json:"error,omitempty"`
}

func Handler(c Caller) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != "POST" || r.URL.Path != "/rpc" {
			http.NotFound(w, r)
			return
		}
		var req Request
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 12<<20))
		if e := dec.Decode(&req); e != nil {
			_ = json.NewEncoder(w).Encode(Response{JSONRPC: "2.0", ID: json.RawMessage("null"), Error: &RPCError{Code: -32700, Message: "invalid JSON"}})
			return
		}
		if dec.Decode(new(any)) != io.EOF {
			_ = json.NewEncoder(w).Encode(Response{JSONRPC: "2.0", ID: json.RawMessage("null"), Error: &RPCError{Code: -32700, Message: "expected one JSON request"}})
			return
		}
		if req.JSONRPC != "2.0" || req.Method == "" || len(req.ID) == 0 {
			_ = json.NewEncoder(w).Encode(Response{JSONRPC: "2.0", ID: json.RawMessage("null"), Error: &RPCError{Code: -32600, Message: "version, method and id are required"}})
			return
		}
		result, e := c.Call(r.Context(), req.Method, req.Params)
		resp := Response{JSONRPC: "2.0", ID: req.ID}
		if e != nil {
			me := model.AsError(e)
			code := -32000
			if me.Code == "METHOD_NOT_FOUND" {
				code = -32601
			}
			if me.Code == "INVALID_ARGUMENT" {
				code = -32602
			}
			resp.Error = &RPCError{Code: code, Message: me.Message, Data: me}
		} else {
			resp.Result, _ = json.Marshal(result)
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
}
func Serve(ctx context.Context, socket string, c Caller) error {
	return ServeWithRemote(ctx, socket, c, RemoteConfig{})
}

type Client struct {
	hosts    *engine.Hosts
	HTTP     *http.Client
	socket   string
	endpoint string
	token    string
}

func NewClient(socket string) *Client {
	tr := &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "unix", socket)
	}}
	return &Client{HTTP: &http.Client{Transport: tr}, socket: socket, endpoint: "http://localhost", hosts: engine.NewHosts(4)}
}
func (c *Client) Close() { c.HTTP.CloseIdleConnections() }
func (c *Client) Call(ctx context.Context, method string, params json.RawMessage) (any, error) {
	if len(params) == 0 {
		params = []byte("{}")
	}
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	b, e := json.Marshal(Request{JSONRPC: "2.0", ID: json.RawMessage("1"), Method: method, Params: params})
	if e != nil {
		return nil, e
	}
	r, e := http.NewRequestWithContext(ctx, "POST", c.endpoint+"/rpc", bytes.NewReader(b))
	if e != nil {
		return nil, e
	}
	r.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		r.Header.Set("Authorization", "Bearer "+c.token)
	}
	response, e := c.HTTP.Do(r)
	if e != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		message := "cannot reach configured node; its task state is unknown"
		if c.socket != "" {
			message = "cannot connect to download service; run fetchra serve"
		}
		return nil, &model.Error{Code: "SERVICE_UNAVAILABLE", Message: message, Retryable: true}
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusUnauthorized {
		return nil, model.Err("AUTH_FAILED", "node rejected the access token")
	}
	if response.StatusCode != 200 {
		return nil, model.Err("RPC_ERROR", "unexpected service response")
	}
	var v Response
	if e = json.NewDecoder(io.LimitReader(response.Body, 32<<20)).Decode(&v); e != nil {
		return nil, model.Err("RPC_ERROR", "invalid service response")
	}
	if v.Error != nil {
		if v.Error.Data != nil {
			return nil, v.Error.Data
		}
		return nil, model.Err("RPC_ERROR", v.Error.Message)
	}
	if v.JSONRPC != "2.0" || string(v.ID) != "1" {
		return nil, model.Err("RPC_ERROR", "invalid response identity")
	}
	var result any
	decoder := json.NewDecoder(bytes.NewReader(v.Result))
	decoder.UseNumber()
	if e = decoder.Decode(&result); e != nil {
		return nil, e
	}
	return result, nil
}

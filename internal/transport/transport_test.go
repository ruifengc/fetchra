package transport

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/RyanChen-cn/fetchra/internal/model"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type stub struct{}

func (stub) Call(ctx context.Context, method string, p json.RawMessage) (any, error) {
	if method == "task_get" {
		return nil, model.Err("TASK_NOT_FOUND", "missing task")
	}
	return map[string]any{"version": "test"}, nil
}
func TestRPCAndMCPParity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	httpServer := httptest.NewServer(Handler(stub{}))
	defer httpServer.Close()
	rpc := NewClient("")
	defer rpc.Close()
	rpc.HTTP = httpServer.Client()
	// Exercise the actual HTTP handler separately from the Unix dialer.
	request := `{"jsonrpc":"2.0","id":1,"method":"service_status","params":{}}`
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/rpc", strings.NewReader(request))
	Handler(stub{}).ServeHTTP(recorder, req)
	var response Response
	if e := json.Unmarshal(recorder.Body.Bytes(), &response); e != nil || response.Error != nil {
		t.Fatal("RPC failed", e)
	}
	server := MCPServer(stub{})
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	ct, st := mcp.NewInMemoryTransports()
	ss, e := server.Connect(ctx, st, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer ss.Close()
	cs, e := client.Connect(ctx, ct, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer cs.Close()
	tools, e := cs.ListTools(ctx, nil)
	if e != nil || len(tools.Tools) != 18 {
		t.Fatal("missing tools", e)
	}
	result, e := cs.CallTool(ctx, &mcp.CallToolParams{Name: "service_status", Arguments: map[string]any{}})
	if e != nil || result.IsError {
		t.Fatal("MCP failed", e)
	}
	b, _ := json.Marshal(result.StructuredContent)
	var envelope struct {
		Result json.RawMessage `json:"result"`
	}
	if e = json.Unmarshal(b, &envelope); e != nil {
		t.Fatal(e)
	}
	if string(response.Result) != string(envelope.Result) {
		t.Fatalf("different RPC and MCP semantics: %s %s", response.Result, envelope.Result)
	}
	bad, e := cs.CallTool(ctx, &mcp.CallToolParams{Name: "task_get", Arguments: map[string]any{"task_id": "missing"}})
	if e != nil || !bad.IsError {
		t.Fatal("domain error not returned as tool error", e)
	}
}

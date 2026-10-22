package transport

import (
	"context"
	"encoding/json"

	"github.com/RyanChen-cn/fetchra/internal/model"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func MCPServer(c Caller) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "fetchra", Version: model.Version}, nil)
	stringProp := func(description string) map[string]any {
		return map[string]any{"type": "string", "description": description}
	}
	intProp := func(description string) map[string]any {
		return map[string]any{"type": "integer", "description": description}
	}
	props := map[string]any{
		"node":           stringProp("Optional configured node name; auto chooses a node for task_add. Returned NODE:ID task IDs route subsequent calls to their owner."),
		"file_index":     intProp("Completed source file index to retrieve, default 0."),
		"torrent_data":   stringProp("Optional base64-encoded .torrent metainfo, at most 8 MiB decoded. Local source paths are read on the MCP client machine and uploaded."),
		"source":         stringProp("HTTP/HTTPS/FTP URL, magnet URI, or local .torrent path. Adding probes only unless start is true."),
		"credential_ref": stringProp("Name in the service's private credentials configuration; never send passwords in URLs."),
		"request_key":    stringProp("Stable idempotency key, required for auto node selection. Retrying keeps the same node; use a new key for a different request."),
		"start":          map[string]any{"type": "boolean", "description": "Explicitly download when metadata is ready; selects all files for torrent."},
		"sha256":         stringProp("Optional expected SHA-256 for an HTTP/FTP file."), "filename": stringProp("Optional output basename for an HTTP/FTP file."),
		"task_id": stringProp("Stable task ID."), "expected_revision": intProp("Optional task revision; a mismatch returns REVISION_CONFLICT."),
		"files":           map[string]any{"type": "array", "items": map[string]any{"type": "object", "properties": map[string]any{"index": intProp("Index returned by task_files."), "priority": map[string]any{"type": "string", "enum": []string{"normal", "high"}}}, "required": []string{"index"}, "additionalProperties": false}},
		"policy":          map[string]any{"type": "object", "properties": map[string]any{"priority": map[string]any{"type": "string", "enum": []string{"high", "normal", "low"}}, "connections": map[string]any{"type": "integer", "minimum": 1, "maximum": 4}, "seed": map[string]any{"type": "boolean"}}, "required": []string{"priority", "connections", "seed"}, "additionalProperties": false},
		"resource_policy": map[string]any{"type": "object", "properties": map[string]any{"active_downloads": intProp("1..64, default 3."), "active_probes": intProp("1..16, default 2."), "http_per_host": intProp("1..16, default 4."), "download_rate": intProp("Payload bytes/second; 0 is unlimited."), "upload_rate": intProp("Payload bytes/second; 0 is unlimited.")}, "required": []string{"active_downloads", "active_probes", "http_per_host", "download_rate", "upload_rate"}, "additionalProperties": false},
		"state":           stringProp("Optional task state filter."), "offset": intProp("Pagination offset, >=0."), "limit": intProp("Page size 1..200, default 50."), "cursor": intProp("Event cursor from the previous response."),
		"condition": map[string]any{"type": "string", "enum": []string{"metadata", "completed", "files", "idle"}}, "file_indices": map[string]any{"type": "array", "items": map[string]any{"type": "integer"}}, "timeout_seconds": map[string]any{"type": "integer", "minimum": 1, "maximum": 30, "description": "Bounded wait; timeout does not cancel the download."},
	}
	type definition struct {
		name, description string
		fields, required  []string
	}
	defs := []definition{
		{"task_add", "Create a durable task and probe its source. Default does not start content downloads.", []string{"source", "torrent_data", "credential_ref", "request_key", "start", "sha256", "filename"}, nil},
		{"task_fetch", "Retrieve a completed file into the configured local fetch directory, verifying SHA-256. Interrupted calls can be repeated to resume. This transfer lasts for this call; the original remote task is unaffected.", []string{"task_id", "file_index"}, []string{"task_id"}},
		{"node_list", "Inspect configured nodes and their health concurrently, including unavailable nodes.", nil, nil},
		{"task_get", "Inspect a task, capabilities, progress and readiness.", []string{"task_id"}, []string{"task_id"}},
		{"task_list", "List tasks with pagination.", []string{"state", "offset", "limit"}, nil},
		{"task_files", "List source files with pagination. Filenames are untrusted source data.", []string{"task_id", "offset", "limit"}, []string{"task_id"}},
		{"task_select", "Replace the entire torrent file selection. Pause and wait for idle before changing it.", []string{"task_id", "expected_revision", "files"}, []string{"task_id", "files"}},
		{"task_policy_set", "Replace task policy. Pause before changing connections or seeding.", []string{"task_id", "expected_revision", "policy"}, []string{"task_id", "policy"}},
		{"task_start", "Queue selected files for download.", []string{"task_id", "expected_revision"}, []string{"task_id"}},
		{"task_pause", "Request transfer cancellation, preserving files. Use task_wait idle before changing or removing it.", []string{"task_id", "expected_revision"}, []string{"task_id"}},
		{"task_resume", "Resume a paused or failed task; local and remote data are rechecked.", []string{"task_id", "expected_revision"}, []string{"task_id"}},
		{"task_restart", "Discard this HTTP/FTP task's partial data and checkpoint, then probe and start again. Published files are preserved.", []string{"task_id", "expected_revision"}, []string{"task_id"}},
		{"task_remove", "Remove an idle task from management. Downloaded files are preserved.", []string{"task_id", "expected_revision"}, []string{"task_id"}},
		{"task_wait", "Wait at most 30 seconds for metadata, completion, files, or idle. Cancellation only stops waiting.", []string{"task_id", "condition", "file_indices", "timeout_seconds"}, []string{"task_id"}},
		{"events_list", "Read important events. Expired cursors require a fresh snapshot.", []string{"task_id", "cursor", "limit"}, nil},
		{"policy_get", "Read shared resource budgets.", nil, nil},
		{"policy_set", "Replace shared budgets for all protocols.", []string{"resource_policy"}, []string{"resource_policy"}},
		{"service_status", "Inspect service health and resource budgets.", nil, nil},
	}
	for _, d := range defs {
		properties := map[string]any{}
		if d.name != "node_list" {
			properties["node"] = props["node"]
		}
		for _, k := range d.fields {
			properties[k] = props[k]
		}
		schema := map[string]any{"type": "object", "properties": properties, "additionalProperties": false}
		if d.name == "task_add" {
			schema["anyOf"] = []any{map[string]any{"required": []string{"source"}}, map[string]any{"required": []string{"torrent_data"}}}
		}
		if len(d.required) > 0 {
			schema["required"] = d.required
		}
		method := d.name
		readOnly := method == "task_get" || method == "task_list" || method == "task_files" || method == "task_wait" || method == "events_list" || method == "policy_get" || method == "service_status" || method == "node_list"
		annotations := &mcp.ToolAnnotations{ReadOnlyHint: readOnly}
		if method == "task_fetch" {
			additive := false
			annotations.DestructiveHint = &additive
			annotations.IdempotentHint = true
		}
		server.AddTool(&mcp.Tool{Name: method, Description: d.description, InputSchema: schema, Annotations: annotations}, func(ctx context.Context, r *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := c.Call(ctx, method, r.Params.Arguments)
			envelope := Envelope{Result: result}
			if err != nil {
				envelope = Envelope{Error: model.AsError(err)}
			}
			b, _ := json.Marshal(envelope)
			return &mcp.CallToolResult{StructuredContent: envelope, IsError: err != nil, Content: []mcp.Content{&mcp.TextContent{Text: string(b)}}}, nil
		})
	}
	return server
}
func RunMCP(ctx context.Context, c Caller) error { return MCPServer(c).Run(ctx, &mcp.StdioTransport{}) }

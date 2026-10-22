package engine

import (
	"bytes"
	"context"
	"github.com/RyanChen-cn/fetchra/internal/model"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkHTTPRange(b *testing.B) {
	data := bytes.Repeat([]byte("x"), int(BlockSize))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"benchmark"`)
		http.ServeContent(w, r, "data", time.Time{}, bytes.NewReader(data))
	}))
	defer server.Close()
	h := testHTTP()
	defer h.Close()
	c := h.client()
	defer c.CloseIdleConnections()
	t := model.Task{Source: model.Source{URL: server.URL}, Metadata: &model.Metadata{FinalURL: server.URL}}
	var transferred atomic.Int64
	b.ReportAllocs()
	b.SetBytes(BlockSize)
	buffer := make([]byte, int(BlockSize)+1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := h.readRange(context.Background(), c, t, 0, BlockSize, BlockSize, `"benchmark"`, &transferred, buffer); err != nil {
			b.Fatal(err)
		}
	}
}

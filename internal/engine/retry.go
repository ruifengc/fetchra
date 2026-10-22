package engine

import (
	"context"
	"errors"
	"github.com/RyanChen-cn/fetchra/internal/model"
	"time"
)

// RetryDownload is the single retry layer for both daemon jobs and file fetches.
// Every attempt recovers durable blocks; only transient errors are retried.
func RetryDownload(ctx context.Context, e Engine, t model.Task, report Report) error {
	for attempt := 0; attempt < 5; attempt++ {
		err := e.Download(ctx, t, report)
		var domain *model.Error
		if err == nil || ctx.Err() != nil || !errors.As(err, &domain) || !domain.Retryable || attempt == 4 {
			return err
		}
		timer := time.NewTimer(time.Second * time.Duration(1<<attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	panic("unreachable retry state")
}

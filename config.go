package batch

import "time"

// Config configuration for a batch
type Config struct {
	maxItems  int
	maxAge    time.Duration
	consumers int
}

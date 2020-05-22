package batch

// Config configuration for a batch
type Config struct {
	maxItems  int
	maxAge    int
	consumers int
}

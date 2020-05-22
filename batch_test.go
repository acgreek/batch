package batch

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOneBatch(t *testing.T) {
	b := NewBatch(Config{2, 1, 10})
	defer b.Close()

	b.Append(1)
	b.Append(2)
	b.Append(3)
	b.Append(4)
	b.Append(5)

	result := b.Scan()
	assert.Len(t, result, 2)
	assert.Equal(t, 1, result[0])
	assert.Equal(t, 2, result[1])
	result = b.Scan()
	assert.Len(t, result, 2)
	assert.Equal(t, 3, result[0])
	assert.Equal(t, 4, result[1])
	result = b.Scan()
	assert.Len(t, result, 1)
	assert.Equal(t, 5, result[0])
}

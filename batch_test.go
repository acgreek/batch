package batch

import (
	"testing"
)

func TestOneBatch(t *testing.T) {
	b := NewBatch(2, 1, 10)
	defer b.Close()

	b.Append(1)
	b.Append(2)
	b.Append(3)
	b.Append(4)
	b.Append(5)

	result := b.Scan()
	if len(result) != 2 {
		t.Fatalf("result was not of length 2. Was %d", len(result))
	}
	if result[0] != 1 {
		t.Fatalf("result[0] was not 1.  %d", result[0])
	}
	if result[1] != 2 {
		t.Fatalf("result[0] was not 1.  %d", result[0])
	}
	result = b.Scan()
	if len(result) != 2 {
		t.Fatalf("result was not of length 2. Was %d", len(result))
	}
	if result[0] != 3 {
		t.Fatalf("result[0] was not 1.  %d", result[0])
	}
	if result[1] != 4 {
		t.Fatalf("result[0] was not 1.  %d", result[0])
	}

	result = b.Scan()
	if len(result) != 1 {
		t.Fatalf("result was not of length 2. Was %d", len(result))
	}
	if result[0] != 5 {
		t.Fatalf("result[0] was not 1.  %d", result[0])
	}
}

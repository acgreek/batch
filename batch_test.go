package main

import "testing"

func TestOneBatch(t *testing.T) {
	b := NewBatch(2, 1,  2)

	b.Append(1)
	b.Append(2)
	b.Append(3)
	b.Append(4)
	b.Append(5)

	result := b.Scan();
	if (len(result) != 2) {
		t.Fatal("result was not of length 2. Was %d", len(result))
	}
	if (result[0] != 1) {
		t.Fatal("result[0] was not 1.  %d", result[0])
	}
	if (result[1] != 2) {
		t.Fatal("result[0] was not 1.  %d", result[0])
	}
	result = b.Scan();
	if (len(result) != 2) {
		t.Fatal("result was not of length 2. Was %d", len(result))
	}
	if (result[0] != 3) {
		t.Fatal("result[0] was not 1.  %d", result[0])
	}
	if (result[1] != 4) {
		t.Fatal("result[0] was not 1.  %d", result[0])
	}
	result = b.Scan();
	if (len(result) != 1) {
		t.Fatal("result was not of length 2. Was %d", len(result))
	}
	if (result[0] != 5) {
		t.Fatal("result[0] was not 1.  %d", result[0])
	}
	b.Close();
}

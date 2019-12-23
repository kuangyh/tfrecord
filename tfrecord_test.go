package tfrecord

import (
	"bytes"
	"os"
	"testing"
)

func TestIO(t *testing.T) {
	records := []string{"Hello", "World!"}

	buf := &bytes.Buffer{}
	w := NewWriter(buf)
	for _, r := range records {
		n, err := w.Write([]byte(r))
		if err != nil {
			t.Errorf("failed wrting %s, %v", r, err)
		}
		if n != len(r) {
			t.Errorf("expect write size %d, actual %d", len(r), n)
		}
	}

	it := NewIterator(bytes.NewReader(buf.Bytes()), 1000, true)
	var read []string
	for it.Next() {
		read = append(read, string(it.Value()))
	}
	if err := it.Err(); err != nil {
		t.Fatalf("read error %v", err)
	}
	if len(records) != len(read) {
		t.Fatalf("unmatched read len, expect %d, actual %d", len(records), len(read))
	}
	for i, v := range records {
		if v != read[i] {
			t.Errorf("unmatched read value idx %d, expect %s, actual %s", i, v, read[i])
		}
	}
}

func TestReadFromTF(t *testing.T) {
	r, err := os.Open("testdata/test.tfrecord")
	if err != nil {
		t.Fatalf("failed opening test file %v", err)
	}
	defer r.Close()
	out := ""
	it := NewIterator(r, 1000, true)
	for it.Next() {
		out += string(it.Value())
	}
	if err := it.Err(); err != nil {
		t.Fatalf("read error %v", err)
	}
	expect := "HelloWorldFromTensorflow"
	if out != expect {
		t.Errorf("unmatched read content, expect %s, acutal %s", expect, out)
	}
}

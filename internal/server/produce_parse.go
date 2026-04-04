package server

import (
	"bufio"
	"encoding/json"
	"io"
	"unsafe"
)

func newBodyDecoder(r io.Reader) (*json.Decoder, byte, error) {
	br := bufio.NewReader(r)
	first, err := peekFirstNonSpaceByte(br)
	if err != nil {
		return nil, 0, err
	}
	return json.NewDecoder(br), first, nil
}

func peekFirstNonSpaceByte(r *bufio.Reader) (byte, error) {
	for {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		switch b {
		case ' ', '\t', '\n', '\r':
			continue
		default:
			if err := r.UnreadByte(); err != nil {
				return 0, err
			}
			return b, nil
		}
	}
}

// immutableStringBytes exposes a string as a read-only byte slice without
// copying. The returned slice must never be mutated.
func immutableStringBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

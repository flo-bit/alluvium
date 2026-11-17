package snapshot

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
)

// Account is one line of accounts.zst, which is sorted by DID. It lists
// every account that has records or is hidden. A build uses it to find the
// collections an account-level event touches, and to remember that an
// account is hidden even while it has no records.
type Account struct {
	DID string `json:"did"`
	// Hidden accounts are deactivated, suspended or taken down at their
	// PDS. Their records are kept in hidden/ and not served.
	Hidden bool `json:"hidden,omitempty"`
	// Collections the account has records in, sorted.
	Collections []string `json:"collections,omitempty"`
}

// accountsFrame is how many bytes of lines go into one zstd frame.
const accountsFrame = 1 << 20

// AccountWriter writes an accounts file. Accounts must arrive in DID order.
type AccountWriter struct {
	f       *os.File
	pending []byte
}

func CreateAccounts(path string) (*AccountWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &AccountWriter{f: f}, nil
}

func (w *AccountWriter) Add(a Account) error {
	line, err := json.Marshal(a)
	if err != nil {
		return err
	}
	w.pending = append(append(w.pending, line...), '\n')
	if len(w.pending) >= accountsFrame {
		return w.flush()
	}
	return nil
}

func (w *AccountWriter) flush() error {
	if len(w.pending) == 0 {
		return nil
	}
	_, err := w.f.Write(Compress(w.pending))
	w.pending = w.pending[:0]
	return err
}

func (w *AccountWriter) Close() error {
	return errors.Join(w.flush(), w.f.Close())
}

// AccountReader reads an accounts file in order.
type AccountReader struct {
	f   *os.File
	dec *zstd.Decoder
	r   *bufio.Reader
}

func OpenAccounts(path string) (*AccountReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	dec, err := zstd.NewReader(f, zstd.WithDecoderConcurrency(1))
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &AccountReader{f: f, dec: dec, r: bufio.NewReaderSize(dec, 1<<16)}, nil
}

// Next returns the next account, or io.EOF after the last.
func (r *AccountReader) Next() (Account, error) {
	line, err := r.r.ReadBytes('\n')
	if err != nil {
		if err == io.EOF && len(line) > 0 {
			err = io.ErrUnexpectedEOF
		}
		return Account{}, err
	}
	var a Account
	return a, json.Unmarshal(line, &a)
}

func (r *AccountReader) Close() error {
	r.dec.Close()
	return r.f.Close()
}

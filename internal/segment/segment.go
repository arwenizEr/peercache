package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// ErrCorrupt reports bytes that are not a well-formed file of this format.
var ErrCorrupt = errors.New("segment: corrupt")

const (
	magic   = "PCSG"
	version = 1

	flagTombstone = 1 << 0

	// minRecord is the fewest bytes one record can occupy: a flags byte,
	// three empty strings, the two fixed-width integers and an empty value.
	minRecord = 1 + 1 + 1 + 1 + 8 + 8 + 1
)

// Kind tells a segment from a snapshot.
type Kind uint8

const (
	// KindSegment is a batch of records one owner flushed.
	KindSegment Kind = 1
	// KindSnapshot is a compactor's merge of a partition, naming the
	// segments it covers.
	KindSnapshot Kind = 2
)

// Record is one key's state as one write left it.
type Record struct {
	Namespace string
	Key       string
	// Writer is the node that made the write; it breaks ties between equal
	// orders, so two records never compare equal unless they are the same.
	Writer string
	// Order is the write's hybrid logical clock value.
	Order uint64
	// ExpiresAt is the owner's absolute deadline in Unix nanoseconds, or
	// zero for none.
	ExpiresAt int64
	// Tombstone marks a deletion. Value is empty when it is set.
	Tombstone bool
	Value     []byte
}

// Newer reports whether r supersedes other: a later order, or an equal order
// from a writer whose ID sorts higher.
func (r *Record) Newer(other *Record) bool {
	if r.Order != other.Order {
		return r.Order > other.Order
	}
	return r.Writer > other.Writer
}

// File is a decoded segment or snapshot.
type File struct {
	Kind    Kind
	Covered []string
	Records []Record
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Encode returns f's bytes.
func Encode(f *File) []byte {
	size := len(magic) + 2 + 2*binary.MaxVarintLen64 + 4
	for _, c := range f.Covered {
		size += binary.MaxVarintLen64 + len(c)
	}
	for i := range f.Records {
		r := &f.Records[i]
		size += 1 + 16 + 4*binary.MaxVarintLen64 + len(r.Namespace) + len(r.Key) + len(r.Writer) + len(r.Value)
	}
	b := make([]byte, 0, size)
	b = append(b, magic...)
	b = append(b, version, byte(f.Kind))
	b = binary.AppendUvarint(b, uint64(len(f.Covered)))
	for _, c := range f.Covered {
		b = appendString(b, c)
	}
	b = binary.AppendUvarint(b, uint64(len(f.Records)))
	for i := range f.Records {
		r := &f.Records[i]
		var flags byte
		if r.Tombstone {
			flags |= flagTombstone
		}
		b = append(b, flags)
		b = appendString(b, r.Namespace)
		b = appendString(b, r.Key)
		b = appendString(b, r.Writer)
		b = binary.LittleEndian.AppendUint64(b, r.Order)
		b = binary.LittleEndian.AppendUint64(b, uint64(r.ExpiresAt))
		b = appendBytes(b, r.Value)
	}
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

func appendString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func appendBytes(b []byte, v []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(v)))
	return append(b, v...)
}

// Decode parses data. Any defect — a bad checksum, an unknown version or kind,
// a length past the end, trailing bytes — is ErrCorrupt.
func Decode(data []byte) (*File, error) {
	if len(data) < len(magic)+2+2+4 {
		return nil, fmt.Errorf("%w: %d bytes is shorter than any file", ErrCorrupt, len(data))
	}
	body, sum := data[:len(data)-4], binary.LittleEndian.Uint32(data[len(data)-4:])
	if crc32.Checksum(body, castagnoli) != sum {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	}
	if string(body[:len(magic)]) != magic {
		return nil, fmt.Errorf("%w: bad magic", ErrCorrupt)
	}
	if body[len(magic)] != version {
		return nil, fmt.Errorf("%w: unknown version %d", ErrCorrupt, body[len(magic)])
	}
	f := &File{Kind: Kind(body[len(magic)+1])}
	if f.Kind != KindSegment && f.Kind != KindSnapshot {
		return nil, fmt.Errorf("%w: unknown kind %d", ErrCorrupt, f.Kind)
	}
	d := decoder{b: body[len(magic)+2:]}

	// Every count is checked against what remains before anything is
	// allocated for it: a covered name costs at least one byte, a record
	// at least minRecord.
	n, ok := d.count(1)
	if !ok {
		return nil, fmt.Errorf("%w: covered count", ErrCorrupt)
	}
	if f.Kind == KindSegment && n != 0 {
		return nil, fmt.Errorf("%w: a segment names covered segments", ErrCorrupt)
	}
	if n > 0 {
		f.Covered = make([]string, 0, n)
	}
	for i := 0; i < n; i++ {
		s, ok := d.str()
		if !ok {
			return nil, fmt.Errorf("%w: covered name %d", ErrCorrupt, i)
		}
		f.Covered = append(f.Covered, s)
	}

	n, ok = d.count(minRecord)
	if !ok {
		return nil, fmt.Errorf("%w: record count", ErrCorrupt)
	}
	if n > 0 {
		f.Records = make([]Record, 0, n)
	}
	for i := 0; i < n; i++ {
		var r Record
		flags, ok := d.byte()
		if !ok || flags&^flagTombstone != 0 {
			return nil, fmt.Errorf("%w: record %d flags", ErrCorrupt, i)
		}
		r.Tombstone = flags&flagTombstone != 0
		if r.Namespace, ok = d.str(); !ok {
			return nil, fmt.Errorf("%w: record %d namespace", ErrCorrupt, i)
		}
		if r.Key, ok = d.str(); !ok {
			return nil, fmt.Errorf("%w: record %d key", ErrCorrupt, i)
		}
		if r.Writer, ok = d.str(); !ok {
			return nil, fmt.Errorf("%w: record %d writer", ErrCorrupt, i)
		}
		if r.Order, ok = d.u64(); !ok {
			return nil, fmt.Errorf("%w: record %d order", ErrCorrupt, i)
		}
		expires, ok := d.u64()
		if !ok {
			return nil, fmt.Errorf("%w: record %d deadline", ErrCorrupt, i)
		}
		r.ExpiresAt = int64(expires)
		if r.Value, ok = d.bytes(); !ok {
			return nil, fmt.Errorf("%w: record %d value", ErrCorrupt, i)
		}
		if r.Tombstone && len(r.Value) != 0 {
			return nil, fmt.Errorf("%w: record %d is a tombstone with a value", ErrCorrupt, i)
		}
		f.Records = append(f.Records, r)
	}
	if len(d.b) != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes", ErrCorrupt, len(d.b))
	}
	return f, nil
}

// decoder reads from b, shrinking it. Every method reports false rather than
// reading past the end.
type decoder struct{ b []byte }

func (d *decoder) uvarint() (uint64, bool) {
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		return 0, false
	}
	d.b = d.b[n:]
	return v, true
}

// count reads a count of items each at least minEach bytes long, refusing one
// the remaining bytes could not hold.
func (d *decoder) count(minEach int) (int, bool) {
	v, ok := d.uvarint()
	if !ok || v > uint64(len(d.b)/minEach) {
		return 0, false
	}
	return int(v), true
}

func (d *decoder) byte() (byte, bool) {
	if len(d.b) < 1 {
		return 0, false
	}
	c := d.b[0]
	d.b = d.b[1:]
	return c, true
}

func (d *decoder) u64() (uint64, bool) {
	if len(d.b) < 8 {
		return 0, false
	}
	v := binary.LittleEndian.Uint64(d.b)
	d.b = d.b[8:]
	return v, true
}

func (d *decoder) bytes() ([]byte, bool) {
	n, ok := d.uvarint()
	if !ok || n > uint64(len(d.b)) {
		return nil, false
	}
	out := make([]byte, n)
	copy(out, d.b[:n])
	d.b = d.b[n:]
	return out, true
}

func (d *decoder) str() (string, bool) {
	n, ok := d.uvarint()
	if !ok || n > uint64(len(d.b)) {
		return "", false
	}
	s := string(d.b[:n])
	d.b = d.b[n:]
	return s, true
}

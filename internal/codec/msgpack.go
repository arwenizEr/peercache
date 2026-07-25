package codec

import (
	"bytes"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

// NameMsgPack is the registry key for the MsgPack codec.
const NameMsgPack = "msgpack"

// msgpackCodec is a binary codec: smaller and faster than JSON, unreadable in a
// log. Name it when payload size or encode cost matters more than being able to
// eyeball a value in a debug dump.
type msgpackCodec struct{}

// Name implements [Codec].
func (msgpackCodec) Name() string { return NameMsgPack }

// Marshal implements [Codec].
func (msgpackCodec) Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	// Struct field names rather than array positions: a value encoded by an
	// old pod must remain decodable by a new one during a rolling upgrade,
	// and positional encoding turns a field insertion into silent
	// corruption.
	enc.SetCustomStructTag("json")
	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("codec: msgpack marshal: %w", err)
	}
	return buf.Bytes(), nil
}

// Unmarshal implements [Codec].
func (msgpackCodec) Unmarshal(data []byte, v any) error {
	dec := msgpack.NewDecoder(bytes.NewReader(data))
	dec.SetCustomStructTag("json")
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("codec: msgpack unmarshal: %w", err)
	}
	return nil
}

func init() { codecs[NameMsgPack] = msgpackCodec{} }

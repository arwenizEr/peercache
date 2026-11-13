// Package segment is the storage format of persistent namespaces: the bytes of
// every segment and snapshot object a [persist.BlobStore] holds.
//
// A file is a header, a list of covered segment names (snapshots only), a list
// of records, and a CRC-32C of everything before it:
//
//	magic "PCSG" | version u8 | kind u8 | covered: uvarint n, n × string
//	           | records: uvarint n, n × record | crc32c u32 (little endian)
//
//	record = flags u8 (bit 0: tombstone) | namespace string | key string
//	       | writer string | order u64 | expiresAt i64 | value bytes
//
// where string and bytes are a uvarint length then that many bytes, and the
// integers are little-endian fixed width.
//
// # Bytes read back are not trusted
//
// Storage outlives the process that wrote it, and disks and object stores
// return corrupted bytes rarely but not never. So decoding is bounded the way
// internal/wire's is: no declared length or count is believed beyond the bytes
// actually remaining, the checksum is verified before anything is decoded, and
// every failure is [ErrCorrupt] rather than a panic or an allocation sized by
// an attacker's — or a cosmic ray's — number.
//
// # The format is a promise
//
// Data written by one release must be readable by the next. The version byte
// exists so that a later format can be added beside this one; this one is never
// changed in place, and a golden file in testdata pins its exact bytes.
//
// See docs/decisions/0029-persistence-as-a-segment-log.md.
package segment

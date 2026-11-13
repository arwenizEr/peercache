// Package s3 is a persist.BlobStore over Amazon S3 and anything that speaks its
// API — MinIO, Cloudflare R2, Ceph RGW, and the S3-compatible endpoints of
// other clouds.
//
// It is its own module so that the AWS SDK is a dependency of programs that
// choose this backend and of nothing else:
//
//	import pcs3 "github.com/arwenizEr/peercache/persist/s3"
//
//	client := s3.NewFromConfig(cfg) // aws-sdk-go-v2, configured as you like
//	store := pcs3.New(client, "my-bucket", "peercache/orders")
//	ns := cache.Namespace("orders", peercache.WithPersistence(store,
//		persist.WithFlushInterval(time.Second)))
//
// # What S3 provides, and what this relies on
//
// A PUT is atomic — a reader sees the whole object or the previous one — and
// since December 2020 S3 is strongly consistent for reads and listings after a
// write. That is everything persist.BlobStore asks. Some S3-compatible stores
// are only eventually consistent for listings; on those a restore may miss an
// object written moments before, and the next restore finds it.
//
// # Cost
//
// Every flush is one PUT per node and every split at most 64 per cluster
// (docs/decisions/0030-one-object-per-flush.md). Pass a flush interval of a
// second or more with persist.WithFlushInterval: the default 100 ms is ten PUTs
// a second per node. docs/api.md prices the choice.
package s3

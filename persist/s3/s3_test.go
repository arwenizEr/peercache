package s3_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/persist"
	"github.com/arwenizEr/peercache/persist/persisttest"
	pcs3 "github.com/arwenizEr/peercache/persist/s3"
)

// fakeS3 is an in-memory S3 with the behaviour the store depends on: atomic
// PUTs, a typed NoSuchKey, prefix listing in key order, and paging — two keys
// a page, so the paginator is exercised by every listing longer than that.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte // bucket + "\x00" + key
	// notFoundCode, if set, makes a missing GetObject an API error with this
	// code instead of the typed NoSuchKey, as some S3-compatible stores do.
	notFoundCode string
	puts         int
}

func newFake() *fakeS3 { return &fakeS3{objects: map[string][]byte{}} }

func k(bucket, key *string) string { return aws.ToString(bucket) + "\x00" + aws.ToString(key) }

func (f *fakeS3) PutObject(ctx context.Context, in *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.objects[k(in.Bucket, in.Key)] = data
	f.puts++
	f.mu.Unlock()
	return &awss3.PutObjectOutput{}, nil
}

func (f *fakeS3) GetObject(ctx context.Context, in *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	data, ok := f.objects[k(in.Bucket, in.Key)]
	f.mu.Unlock()
	if !ok {
		if f.notFoundCode != "" {
			return nil, &smithy.GenericAPIError{Code: f.notFoundCode, Message: "absent"}
		}
		return nil, &types.NoSuchKey{}
	}
	return &awss3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(bytes.Clone(data)))}, nil
}

func (f *fakeS3) ListObjectsV2(ctx context.Context, in *awss3.ListObjectsV2Input, _ ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	bucket := aws.ToString(in.Bucket) + "\x00"
	f.mu.Lock()
	var keys []string
	for full := range f.objects {
		if key, ok := strings.CutPrefix(full, bucket); ok && strings.HasPrefix(key, aws.ToString(in.Prefix)) {
			keys = append(keys, key)
		}
	}
	f.mu.Unlock()
	sort.Strings(keys)

	start := 0
	if tok := aws.ToString(in.ContinuationToken); tok != "" {
		start, _ = strconv.Atoi(tok)
	}
	end := start + 2
	if end > len(keys) {
		end = len(keys)
	}
	out := &awss3.ListObjectsV2Output{}
	for _, key := range keys[start:end] {
		out.Contents = append(out.Contents, types.Object{Key: aws.String(key)})
	}
	if end < len(keys) {
		out.IsTruncated = aws.Bool(true)
		out.NextContinuationToken = aws.String(strconv.Itoa(end))
	}
	return out, nil
}

func (f *fakeS3) DeleteObject(ctx context.Context, in *awss3.DeleteObjectInput, _ ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	delete(f.objects, k(in.Bucket, in.Key))
	f.mu.Unlock()
	return &awss3.DeleteObjectOutput{}, nil
}

var _ pcs3.API = (*fakeS3)(nil)
var _ pcs3.API = (*awss3.Client)(nil)

func TestTheStoreMeetsTheContract(t *testing.T) {
	for _, prefix := range []string{"", "peercache/orders"} {
		t.Run(fmt.Sprintf("prefix=%q", prefix), func(t *testing.T) {
			persisttest.Run(t, func(*testing.T) persist.BlobStore {
				return pcs3.New(newFake(), "bucket", prefix)
			})
		})
	}
}

// TestAPrefixKeepsStoresApart: two stores in one bucket under different
// prefixes see only their own objects, and names come back without the prefix.
func TestAPrefixKeepsStoresApart(t *testing.T) {
	fake := newFake()
	a := pcs3.New(fake, "bucket", "a")
	b := pcs3.New(fake, "bucket", "/b/")
	ctx := context.Background()
	_ = a.Put(ctx, "orders/p001/x", []byte("a"))
	_ = b.Put(ctx, "orders/p001/x", []byte("b"))
	names, err := a.List(ctx, "")
	if err != nil || len(names) != 1 || names[0] != "orders/p001/x" {
		t.Fatalf("a lists %v, %v", names, err)
	}
	if v, _ := b.Get(ctx, "orders/p001/x"); string(v) != "b" {
		t.Fatalf("b reads %q", v)
	}
	if _, ok := fake.objects["bucket\x00b/orders/p001/x"]; !ok {
		t.Fatalf("a slash-wrapped prefix was not normalised: %v", fake.objects)
	}
}

// TestNotFoundIsRecognisedEitherWay: the typed NoSuchKey and an API error
// coded NotFound are both ErrNotFound, and deleting an absent object is fine.
func TestNotFoundIsRecognisedEitherWay(t *testing.T) {
	for _, code := range []string{"", "NoSuchKey", "NotFound"} {
		fake := newFake()
		fake.notFoundCode = code
		s := pcs3.New(fake, "bucket", "")
		if _, err := s.Get(context.Background(), "absent"); !errors.Is(err, persist.ErrNotFound) {
			t.Fatalf("code %q: Get = %v, want ErrNotFound", code, err)
		}
	}
}

// TestOtherErrorsAreNotNotFound: an access-denied is an error, not a miss.
// Mistaking it for one would make a restore treat a key as absent.
func TestOtherErrorsAreNotNotFound(t *testing.T) {
	fake := newFake()
	fake.notFoundCode = "AccessDenied"
	_, err := pcs3.New(fake, "bucket", "").Get(context.Background(), "absent")
	if err == nil || errors.Is(err, persist.ErrNotFound) {
		t.Fatalf("Get with AccessDenied = %v; it must be an error and not ErrNotFound", err)
	}
}

// TestAgainstARealEndpoint runs the contract against a real S3 or MinIO when
// PEERCACHE_S3_ENDPOINT, PEERCACHE_S3_BUCKET, PEERCACHE_S3_ACCESS_KEY and
// PEERCACHE_S3_SECRET_KEY are set, under a fresh prefix per subtest. It is
// skipped otherwise, which is the default: a unit test does not need the
// network.
func TestAgainstARealEndpoint(t *testing.T) {
	endpoint := os.Getenv("PEERCACHE_S3_ENDPOINT")
	bucket := os.Getenv("PEERCACHE_S3_BUCKET")
	if endpoint == "" || bucket == "" {
		t.Skip("PEERCACHE_S3_ENDPOINT and PEERCACHE_S3_BUCKET not set")
	}
	region := os.Getenv("PEERCACHE_S3_REGION")
	if region == "" {
		region = "us-east-1"
	}
	access, secret := os.Getenv("PEERCACHE_S3_ACCESS_KEY"), os.Getenv("PEERCACHE_S3_SECRET_KEY")
	client := awss3.New(awss3.Options{
		BaseEndpoint: aws.String(endpoint),
		Region:       region,
		UsePathStyle: true, // MinIO and most self-hosted stores
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: access, SecretAccessKey: secret}, nil
		}),
	})
	n := 0
	persisttest.Run(t, func(t *testing.T) persist.BlobStore {
		n++
		prefix := fmt.Sprintf("peercache-contract/%d/%s", n, strings.ReplaceAll(t.Name(), "/", "_"))
		s := pcs3.New(client, bucket, prefix)
		t.Cleanup(func() {
			names, _ := s.List(context.Background(), "")
			for _, name := range names {
				_ = s.Delete(context.Background(), name)
			}
		})
		return s
	})
}

// TestACacheRestoresThroughS3: a persistent namespace over this backend,
// written, closed and reopened by a new cache, reads its keys back — and one
// flush of keys spread over every partition was one PUT.
func TestACacheRestoresThroughS3(t *testing.T) {
	fake := newFake()
	store := pcs3.New(fake, "bucket", "cluster-1")
	ctx := context.Background()

	first, err := peercache.New(peercache.WithNodeID("pod-0"))
	if err != nil {
		t.Fatal(err)
	}
	ns := first.Namespace("orders", peercache.WithPersistence(store))
	for i := 0; i < 500; i++ {
		if err := ns.Set(ctx, fmt.Sprintf("o:%d", i), []byte(fmt.Sprint(i)), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := first.Close(ctx); err != nil { // Close flushes
		t.Fatalf("Close: %v", err)
	}
	if fake.puts != 1 {
		t.Fatalf("500 keys over every partition, flushed once, made %d PUTs; want 1", fake.puts)
	}

	second, err := peercache.New(peercache.WithNodeID("pod-1"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Close(ctx) }()
	ns2 := second.Namespace("orders", peercache.WithPersistence(store))
	for i := 0; i < 500; i++ {
		if v, err := ns2.Get(ctx, fmt.Sprintf("o:%d", i)); err != nil || string(v) != fmt.Sprint(i) {
			t.Fatalf("o:%d after a restart over S3 = %q, %v", i, v, err)
		}
	}
}

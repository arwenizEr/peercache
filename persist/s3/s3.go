package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/arwenizEr/peercache/persist"
)

// API is the part of the S3 client this store uses. *s3.Client from
// aws-sdk-go-v2 satisfies it; a test can pass a fake.
type API interface {
	PutObject(ctx context.Context, in *awss3.PutObjectInput, opts ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
	GetObject(ctx context.Context, in *awss3.GetObjectInput, opts ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
	ListObjectsV2(ctx context.Context, in *awss3.ListObjectsV2Input, opts ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error)
	DeleteObject(ctx context.Context, in *awss3.DeleteObjectInput, opts ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error)
}

// New returns a store keeping objects in bucket under prefix. An empty prefix
// uses the bucket's root; otherwise every object is at "<prefix>/<name>", so
// one bucket can hold several namespaces' stores, or other data.
//
// Every node of a cluster must use the same bucket and prefix.
func New(client API, bucket, prefix string) persist.BlobStore {
	prefix = strings.Trim(prefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	return &store{client: client, bucket: bucket, prefix: prefix}
}

type store struct {
	client API
	bucket string
	prefix string
}

func (s *store) key(name string) string { return s.prefix + name }

func (s *store) Put(ctx context.Context, name string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := s.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(s.key(name)),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	})
	if err != nil {
		return fmt.Errorf("persist/s3: putting %s: %w", name, err)
	}
	return nil
}

func (s *store) Get(ctx context.Context, name string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(name)),
	})
	if err != nil {
		if notFound(err) {
			return nil, fmt.Errorf("persist/s3: %q: %w", name, persist.ErrNotFound)
		}
		return nil, fmt.Errorf("persist/s3: getting %s: %w", name, err)
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("persist/s3: reading %s: %w", name, err)
	}
	return data, nil
}

func (s *store) List(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var names []string
	p := awss3.NewListObjectsV2Paginator(s.client, &awss3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(s.key(prefix)),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("persist/s3: listing %s: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			names = append(names, strings.TrimPrefix(aws.ToString(obj.Key), s.prefix))
		}
	}
	// S3 already lists in UTF-8 binary order, which is Go's string order;
	// sorted again so that an S3-compatible store that does not is still
	// within the contract.
	sort.Strings(names)
	return names, nil
}

func (s *store) Delete(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := s.client.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(name)),
	})
	if err != nil && !notFound(err) {
		return fmt.Errorf("persist/s3: deleting %s: %w", name, err)
	}
	return nil
}

// notFound reports an absent object, however the service phrased it: the SDK's
// typed NoSuchKey, or an API error coded NoSuchKey or NotFound, which is what a
// HEAD-style response and several S3-compatible stores return.
func notFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var api smithy.APIError
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return true
		}
	}
	return false
}

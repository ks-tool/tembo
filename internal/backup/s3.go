// Package backup stores physical backups in an S3 bucket. An upload streams
// from a reader in parts, so a backup goes to the bucket as it is produced, and
// a restore streams back out of it.
package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Options configure the bucket. Credentials come from the standard AWS chain:
// AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, a profile, or an instance role.
type Options struct {
	Bucket    string
	Endpoint  string // empty for AWS; the address of an S3-compatible server otherwise
	Region    string // empty means us-east-1
	PathStyle bool   // bucket in the path, for S3-compatible servers that need it
	PartSize  int64  // bytes per part of an upload; the minimum S3 accepts is 5 MiB
}

// Store is a bucket that backups are written to and read from.
type Store struct {
	client   *s3.Client
	bucket   string
	partSize int64
}

// MinPartSize is the smallest part S3 accepts, except for the last one.
const MinPartSize = 5 << 20

// DefaultPartSize keeps a backup of tens of gigabytes within S3's limit of
// 10000 parts.
const DefaultPartSize = 64 << 20

// keyRe is the object names a backup can have: the characters S3 and a path
// both take without quoting.
var keyRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]*$`)

// ValidKey reports whether key may name a backup object.
func ValidKey(key string) bool { return len(key) <= 1024 && keyRe.MatchString(key) }

// Open makes a Store for the bucket in opts.
func Open(ctx context.Context, opts Options) (*Store, error) {
	if opts.Bucket == "" {
		return nil, errors.New("no bucket configured")
	}
	region := opts.Region
	if region == "" {
		region = "us-east-1"
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if opts.Endpoint != "" {
			o.BaseEndpoint = aws.String(opts.Endpoint)
		}
		o.UsePathStyle = opts.PathStyle
	})
	partSize := opts.PartSize
	if partSize == 0 {
		partSize = DefaultPartSize
	}
	if partSize < MinPartSize {
		return nil, fmt.Errorf("part size %d is below the S3 minimum of %d", partSize, MinPartSize)
	}
	return &Store{client: client, bucket: opts.Bucket, partSize: partSize}, nil
}

// Put uploads everything r holds as the object key and returns its size.
func (s *Store) Put(ctx context.Context, key string, r io.Reader) (int64, error) {
	if !ValidKey(key) {
		return 0, fmt.Errorf("bad backup name %q", key)
	}
	up := manager.NewUploader(s.client, func(u *manager.Uploader) {
		u.PartSize = s.partSize
		u.Concurrency = 1 // one part in memory at a time: the reader is a stream
	})
	if _, err := up.Upload(ctx, &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key), Body: r}); err != nil {
		return 0, err
	}
	// The manager does not report the size; a HEAD does.
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return 0, err
	}
	return aws.ToInt64(head.ContentLength), nil
}

// IsNotFound reports whether err says the object does not exist.
func IsNotFound(err error) bool {
	var nsk *types.NoSuchKey
	return errors.As(err, &nsk)
}

// Get opens the object key for reading. The caller closes it.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if !ValidKey(key) {
		return nil, fmt.Errorf("bad backup name %q", key)
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

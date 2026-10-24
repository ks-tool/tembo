package backup

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"os"
	"testing"
)

func TestValidKey(t *testing.T) {
	for key, want := range map[string]bool{
		"base.tar":           true,
		"nodes/a/2026-10-24": true,
		"":                   false,
		"/abs":               false,
		"a b":                false,
		"../escape":          false,
		"a?query=1":          false,
	} {
		if got := ValidKey(key); got != want {
			t.Errorf("ValidKey(%q) = %v, want %v", key, got, want)
		}
	}
}

func TestOpenNeedsBucketAndAPartSizeS3Accepts(t *testing.T) {
	if _, err := Open(context.Background(), Options{}); err == nil {
		t.Fatal("no bucket: want an error")
	}
	if _, err := Open(context.Background(), Options{Bucket: "b", PartSize: MinPartSize - 1}); err == nil {
		t.Fatal("a part below the S3 minimum: want an error")
	}
}

// TestS3RoundTrip writes a backup of three parts and reads it back. It needs a
// bucket: TEMBO_TEST_S3_ENDPOINT, TEMBO_TEST_S3_BUCKET and the AWS credentials
// in the environment, e.g. a MinIO container. Without them it is skipped.
func TestS3RoundTrip(t *testing.T) {
	endpoint := os.Getenv("TEMBO_TEST_S3_ENDPOINT")
	bucket := os.Getenv("TEMBO_TEST_S3_BUCKET")
	if endpoint == "" || bucket == "" {
		t.Skip("TEMBO_TEST_S3_ENDPOINT and TEMBO_TEST_S3_BUCKET are not set")
	}
	ctx := context.Background()
	store, err := Open(ctx, Options{Bucket: bucket, Endpoint: endpoint, PathStyle: true, PartSize: MinPartSize})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	data := make([]byte, 12<<20) // three parts of 5 MiB
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	const key = "test/round-trip.tar"
	n, err := store.Put(ctx, key, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if n != int64(len(data)) {
		t.Fatalf("Put reported %d bytes, wrote %d", n, len(data))
	}

	body, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer func() { _ = body.Close() }()
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("the object reads back different: %d bytes, want %d", len(got), len(data))
	}

	if _, err := store.Get(ctx, "test/missing.tar"); !IsNotFound(err) {
		t.Fatalf("a missing object: IsNotFound = false, err = %v", err)
	}
}

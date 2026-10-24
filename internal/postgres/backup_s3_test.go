package postgres

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// Without a bucket the S3 endpoints refuse before touching Postgres.
func TestS3BackupWithoutBucketIsUnavailable(t *testing.T) {
	mux := http.NewServeMux()
	opsHandlers(mux, nil, &Instance{DataDir: t.TempDir()}, "a")
	for _, target := range []string{"/backup-s3?key=base.tar", "/restore-backup-s3?key=base.tar"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, target, nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("POST %s without a bucket = %d, want 503", target, rec.Code)
		}
	}
}

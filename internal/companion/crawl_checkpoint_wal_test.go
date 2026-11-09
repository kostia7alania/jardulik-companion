package companion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
)

func checkpointSQLiteCode(err error) int {
	var coded interface{ Code() int }
	if errors.As(err, &coded) {
		return coded.Code()
	}
	return 0
}

func TestCrawlCheckpointReservesWALWriterBeforeSnapshot(t *testing.T) {
	crawler, _, _, httpCalls := crawlFixture(t, func(int, int) ([]Profile, int) { t.Fatal("checkpoint attempted source HTTP"); return nil, 0 })
	ctx := context.Background()
	job, err := crawler.Request(ctx, 1, false)
	if err != nil {
		t.Fatal(err)
	}
	store := crawler.Service.Store
	if err = store.SetMeta(ctx, "checkpoint_test_poster", "initial"); err != nil {
		t.Fatal(err)
	}
	u := url.URL{Scheme: "file", Path: filepath.Join(crawler.Service.Config.DataDir, "jardulik.sqlite")}
	q := url.Values{"_pragma": {"busy_timeout(0)"}}
	u.RawQuery = q.Encode()
	poster, err := sql.Open("sqlite", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer poster.Close()
	poster.SetMaxOpenConns(1)
	var journal string
	if err = poster.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil || journal != "wal" {
		t.Fatal("fixture is not WAL", journal, err)
	}

	// Reproduce the former deferred read→write transaction with a second writer.
	old, err := store.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	var page int
	if err = old.QueryRowContext(ctx, "SELECT page FROM feed_crawls WHERE owner_id=1").Scan(&page); err != nil {
		old.Rollback()
		t.Fatal(err)
	}
	if _, err = poster.ExecContext(ctx, "UPDATE meta SET value='first_commit' WHERE key='checkpoint_test_poster'"); err != nil {
		old.Rollback()
		t.Fatal(err)
	}
	_, upgradeErr := old.ExecContext(ctx, "UPDATE feed_crawls SET page=page WHERE owner_id=1")
	if err = old.Rollback(); err != nil {
		t.Fatal(err)
	}
	if checkpointSQLiteCode(upgradeErr) != 517 {
		t.Fatal("former sequence did not expose SQLITE_BUSY_SNAPSHOT", upgradeErr)
	}

	var blockedWriter error
	err = crawler.checkpointTx(ctx, job, func(tx *sql.Tx, next *CrawlJob) error {
		_, blockedWriter = poster.ExecContext(ctx, "UPDATE meta SET value='should_not_commit' WHERE key='checkpoint_test_poster'")
		next.Page++
		next.Pages++
		return nil
	})
	if err != nil {
		t.Fatal("checkpoint still failed after concurrent writer attempt", err)
	}
	if checkpointSQLiteCode(blockedWriter) != 5 {
		t.Fatal("checkpoint did not reserve the writer before its snapshot", blockedWriter)
	}
	current := crawlJob(t, crawler)
	if current.Page != job.Page+1 || current.Pages != job.Pages+1 || current.State != "running" {
		t.Fatal("checkpoint did not commit its own cursor", current)
	}
	if store.Meta(ctx, "checkpoint_test_poster") != "first_commit" || *httpCalls != 0 {
		t.Fatal("blocked writer committed or checkpoint repeated source HTTP")
	}
}

type checkpointTestError struct{}

func (*checkpointTestError) Error() string { return "private-token-value must not be logged" }
func (*checkpointTestError) Code() int     { return 517 }

func TestCrawlCheckpointFailureKeepsSafeOriginalDiagnostics(t *testing.T) {
	crawler, _, _, httpCalls := crawlFixture(t, func(int, int) ([]Profile, int) { t.Fatal("checkpoint failure requested source HTTP"); return nil, 0 })
	ctx := context.Background()
	job, err := crawler.Request(ctx, 1, false)
	if err != nil {
		t.Fatal(err)
	}
	var log bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&log, nil)))
	defer slog.SetDefault(previous)
	cause := &checkpointTestError{}
	if err = crawler.checkpointFailed(ctx, job, 16943, "hydrate_profile", cause); err != nil {
		t.Fatal("persisted terminal job failure stopped the crawl worker", err)
	}
	current := crawlJob(t, crawler)
	if current.State != "stopped" || current.Reason != "checkpoint_failed" || current.Page != job.Page {
		t.Fatal("failure advanced or lost cursor", current)
	}
	var record map[string]any
	if err = json.Unmarshal(bytes.TrimSpace(log.Bytes()), &record); err != nil {
		t.Fatal(err)
	}
	if record["sqlite_code"] != float64(517) || record["run_id"] != job.RunID || record["target"] != float64(16943) || record["stage"] != "hydrate_profile" || strings.Contains(log.String(), "private-token-value") || *httpCalls != 0 {
		t.Fatal("unsafe or missing checkpoint diagnostics", record)
	}
	if _, err = crawler.Service.Store.DB.ExecContext(ctx, "UPDATE feed_crawls SET state='running',reason='' WHERE owner_id=1"); err != nil {
		t.Fatal(err)
	}
	if _, err = crawler.Service.Store.DB.ExecContext(ctx, `CREATE TRIGGER fail_checkpoint_stop BEFORE UPDATE OF state ON feed_crawls BEGIN SELECT RAISE(ABORT,'stop failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err = crawler.checkpointFailed(ctx, job, 16943, "hydrate_profile", cause); !errors.Is(err, cause) {
		t.Fatal("stop failure lost original cause", err)
	}
}

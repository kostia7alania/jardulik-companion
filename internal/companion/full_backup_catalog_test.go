package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func fullCatalogInsert(t *testing.T, s *Store, country, city string, page int, profiles []Profile) {
	t.Helper()
	raw, err := json.Marshal(profiles)
	if err != nil {
		t.Fatal(err)
	}
	if profiles == nil {
		raw = []byte("[]")
	}
	_, err = s.DB.ExecContext(context.Background(), `INSERT INTO location_batches(owner_id,run_id,upstream_id,generation,country,city,source_page,page_size,total_count,raw_json,source_sha,captured_at)
 VALUES(123,lower(hex(randomblob(8))),500,'generation',?,?,?,20,100,?,?,1)`, country, city, page, string(raw), digest(raw))
	if err != nil {
		t.Fatal(err)
	}
}

func TestFullBackupFingerprintTracksLocationHistoryAndHeads(t *testing.T) {
	ctx := context.Background()
	base := func() []Profile {
		return []Profile{
			{"userId": 501, "name": "First", "miniPhotos": "first.webp", "type": 1, "totalCount": 100, "pageCount": 20},
			{"userId": 502, "name": "Second", "miniPhotos": "second.webp", "type": 0, "totalCount": 100, "pageCount": 20},
		}
	}
	cases := []struct {
		name   string
		setupB bool
		change func(*testing.T, *Store)
		dirty  bool
	}{
		{name: "repeat scan metadata and rendering", change: func(t *testing.T, s *Store) {
			p := base()
			for _, entry := range p {
				for _, key := range []string{"distance", "line", "randomAngle", "totalCount", "pageCount"} {
					entry[key] = 999
				}
			}
			fullCatalogInsert(t, s, "Country", "City", 1, p)
			if _, err := s.DB.ExecContext(ctx, "UPDATE location_batches SET generation='rotated',upstream_id=900,captured_at=999999"); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "reordered entries", change: func(t *testing.T, s *Store) {
			p := base()
			fullCatalogInsert(t, s, "Country", "City", 1, []Profile{p[1], p[0]})
		}},
		{name: "new name", change: func(t *testing.T, s *Store) {
			p := base()
			p[0]["name"] = "Changed"
			fullCatalogInsert(t, s, "Country", "City", 1, p)
		}, dirty: true},
		{name: "new thumbnail", change: func(t *testing.T, s *Store) {
			p := base()
			p[0]["miniPhotos"] = "new.webp"
			fullCatalogInsert(t, s, "Country", "City", 1, p)
		}, dirty: true},
		{name: "new source ID", change: func(t *testing.T, s *Store) {
			p := base()
			p[0]["userId"] = 503
			fullCatalogInsert(t, s, "Country", "City", 1, p)
		}, dirty: true},
		{name: "new profile type", change: func(t *testing.T, s *Store) {
			p := base()
			p[0]["type"] = 0
			fullCatalogInsert(t, s, "Country", "City", 1, p)
		}, dirty: true},
		{name: "member disappeared", change: func(t *testing.T, s *Store) {
			fullCatalogInsert(t, s, "Country", "City", 1, base()[:1])
		}, dirty: true},
		{name: "empty latest page", change: func(t *testing.T, s *Store) {
			fullCatalogInsert(t, s, "Country", "City", 1, nil)
		}, dirty: true},
		{name: "different country", change: func(t *testing.T, s *Store) {
			fullCatalogInsert(t, s, "Other", "City", 1, base())
		}, dirty: true},
		{name: "different city", change: func(t *testing.T, s *Store) {
			fullCatalogInsert(t, s, "Country", "Other", 1, base())
		}, dirty: true},
		{name: "different page", change: func(t *testing.T, s *Store) {
			fullCatalogInsert(t, s, "Country", "City", 2, base())
		}, dirty: true},
		{name: "return to historical head", setupB: true, change: func(t *testing.T, s *Store) {
			fullCatalogInsert(t, s, "Country", "City", 1, base())
		}, dirty: true},
		{name: "new history retained after head returns", change: func(t *testing.T, s *Store) {
			p := base()
			p[0]["name"] = "Interim"
			fullCatalogInsert(t, s, "Country", "City", 1, p)
			fullCatalogInsert(t, s, "Country", "City", 1, base())
		}, dirty: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, s := archiveTestStore(t)
			if _, err := s.DB.ExecContext(ctx, locationSchema); err != nil {
				t.Fatal(err)
			}
			fullCatalogInsert(t, s, "Country", "City", 1, base())
			if tc.setupB {
				p := base()
				p[0]["name"] = "Changed"
				fullCatalogInsert(t, s, "Country", "City", 1, p)
			}
			before := fullTestFingerprint(t, s)
			tc.change(t, s)
			after := fullTestFingerprint(t, s)
			if (before != after) != tc.dirty {
				t.Fatalf("dirty=%v, want %v", before != after, tc.dirty)
			}
			var versions int
			if err := s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM versions").Scan(&versions); err != nil || versions != 0 {
				t.Fatalf("fixture must have previews only: versions=%d err=%v", versions, err)
			}
		})
	}
}

func TestFullBackupFingerprintTracksAutolikePreferences(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name, update string
		dirty        bool
	}{
		{"mode", "mode='automatic'", true},
		{"interval", "interval_seconds=60", true},
		{"series count", "series_count=15", true},
		{"daily cap", "daily_cap=30", true},
		{"run progress", "nonce='new',run_id='new',generation='new',state='waiting',reason='quota',page=3,sent=5,next_attempt=999999,run_limit=5", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, s := archiveTestStore(t)
			if _, err := s.DB.ExecContext(ctx, autoSchema+"INSERT INTO autolike_settings(owner_id,nonce) VALUES(123,'original')"); err != nil {
				t.Fatal(err)
			}
			before := fullTestFingerprint(t, s)
			if _, err := s.DB.ExecContext(ctx, "UPDATE autolike_settings SET "+tc.update); err != nil {
				t.Fatal(err)
			}
			if dirty := before != fullTestFingerprint(t, s); dirty != tc.dirty {
				t.Fatalf("dirty=%v, want %v", dirty, tc.dirty)
			}
		})
	}
}

func TestFullBackupFingerprintOptionalCatalogTablesAndOriginals(t *testing.T) {
	ctx := context.Background()
	_, s := archiveTestStore(t)
	before := fullTestFingerprint(t, s)
	for _, table := range []string{"location_batches", "autolike_settings"} {
		if exists, err := snapshotHasTable(ctx, s.DB, table); err != nil || exists {
			t.Fatalf("fingerprint created optional table %s: exists=%v err=%v", table, exists, err)
		}
	}
	if _, err := s.DB.ExecContext(ctx, locationSchema+autoSchema); err != nil {
		t.Fatal(err)
	}
	if fullTestFingerprint(t, s) != before {
		t.Fatal("empty optional tables dirtied backup")
	}
	raw := `[ { "userId":501, "name":"Original", "miniPhotos":"preview.webp", "totalCount":1, "distance":99, "line":"render" } ]`
	if _, err := s.DB.ExecContext(ctx, `INSERT INTO location_batches(owner_id,run_id,upstream_id,generation,country,city,source_page,page_size,total_count,raw_json,source_sha,captured_at)
 VALUES(123,'run',500,'gen','Country','City',1,20,1,?,'source-sha',1)`, raw); err != nil {
		t.Fatal(err)
	}
	fullTestFingerprint(t, s)
	var saved string
	if err := s.DB.QueryRowContext(ctx, "SELECT raw_json FROM location_batches").Scan(&saved); err != nil || saved != raw {
		t.Fatalf("original changed: %q err=%v", saved, err)
	}
}

func TestFullBackupLocationFingerprintRejectsInvalidSource(t *testing.T) {
	ctx := context.Background()
	for i, raw := range []string{
		`[{"userId":0,"totalCount":1}]`,
		`[{"userId":1}]`,
		`[] trailing`,
		strings.Repeat(" ", serviceManifestLimit+1),
	} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			_, s := archiveTestStore(t)
			if _, err := s.DB.ExecContext(ctx, locationSchema); err != nil {
				t.Fatal(err)
			}
			fullCatalogInsert(t, s, "Country", "City", 1, nil)
			if _, err := s.DB.ExecContext(ctx, "UPDATE location_batches SET raw_json=?", raw); err != nil {
				t.Fatal(err)
			}
			_, err := fullSourceFingerprint(ctx, s.DB)
			if err == nil || (len(raw) > serviceManifestLimit && !errors.Is(err, errArchiveBudget)) {
				t.Fatalf("invalid source accepted or wrong budget error: %v", err)
			}
		})
	}
}

func TestFullBackupFingerprintTracksConfirmedQuotaOutcomes(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name, setup, change string
		dirty               bool
	}{
		{"quota confirmed", "", "INSERT INTO actions(owner_id,profile_id,kind,state,response) VALUES(123,501,'Like','quota_waiting','quota response')", true},
		{"quota retried", "INSERT INTO actions(owner_id,profile_id,kind,state) VALUES(123,501,'Like','quota_waiting')", "UPDATE actions SET state='quota_retried'", true},
		{"quota retry schedule", "INSERT INTO actions(owner_id,profile_id,kind,state) VALUES(123,501,'Like','quota_waiting')", "UPDATE actions SET retry_at=999999", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, s := archiveTestStore(t)
			if tc.setup != "" {
				if _, err := s.DB.ExecContext(ctx, tc.setup); err != nil {
					t.Fatal(err)
				}
			}
			before := fullTestFingerprint(t, s)
			if _, err := s.DB.ExecContext(ctx, tc.change); err != nil {
				t.Fatal(err)
			}
			if dirty := before != fullTestFingerprint(t, s); dirty != tc.dirty {
				t.Fatalf("dirty=%v, want %v", dirty, tc.dirty)
			}
		})
	}
}

package companion

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func TestProfileAgeOrderUsesExactSourceCalendarDay(t *testing.T) {
	for _, tc := range []struct {
		raw  any
		want string
	}{
		{"2000-02-29", "2000-02-29"},
		{"2000-02-29T00:30:00+14:00", "2000-02-29"},
		{"2000-02-29T23:30:00-07:00", "2000-02-29"},
		{" 2000-02-29T00:00:00 ", "2000-02-29"},
		{nil, ""}, {true, ""}, {"0001-01-01T00:00:00Z", ""},
		{"2000-02-30", ""}, {"2000-02-29Tgarbage", ""},
	} {
		if got := profileAgeOrderKey(tc.raw); got != tc.want {
			t.Fatalf("birthday %v: got %q, want %q", tc.raw, got, tc.want)
		}
	}
}

func ageIndexLabels(t *testing.T, page ArchiveAgePage) []string {
	t.Helper()
	if textUnits(page.Text) > 4096 || validMessageEntities(page.Text, page.Entities) != nil {
		t.Fatal("age index exceeds Telegram text/entity limits")
	}
	var labels []string
	for _, entity := range page.Entities {
		prefix, _ := telegramPrefix(page.Text, entity.Offset+entity.Length)
		_, label := telegramPrefix(prefix, entity.Offset)
		labels = append(labels, label)
	}
	return labels
}

func TestArchiveAgeIndexScopesLatestOwnerVersionAndPaginatesTopicLinks(t *testing.T) {
	c, s := archiveTestStore(t)
	ctx := context.Background()
	var profiles []Profile
	for i := range 25 {
		p := Profile{"userId": int64(1000 + i), "name": fmt.Sprintf("Name%d🙂", i), "age": 26, "height": 165}
		if i > 1 {
			p["birthday"] = fmt.Sprintf("2000-11-%02dT00:30:00+14:00", i)
		} else if i == 1 {
			p["birthday"] = "0001-01-01T00:00:00Z"
		}
		profiles = append(profiles, p)
	}
	profiles[24]["name"] = strings.Repeat("🙂", 200) + "\nlong-name"
	if _, err := s.Archive(ctx, 123, profiles, "age-index-test"); err != nil {
		t.Fatal(err)
	}
	for i := range 25 {
		if _, err := s.DB.Exec(`INSERT INTO mirror_topics(profile_id,thread_id) VALUES(?,?)`, 1000+i, 100+i); err != nil {
			t.Fatal(err)
		}
	}
	// A -> B -> A makes MAX(version_id) stale; queue keeps the current source version.
	if _, err := s.Archive(ctx, 123, []Profile{{"userId": 1024, "name": "stale-B", "birthday": "1980-01-01", "age": 46}}, "age-index-test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Archive(ctx, 123, profiles[24:], "age-index-test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Archive(ctx, 123, []Profile{{"userId": 2000, "name": "no-topic", "birthday": "2010-01-01"}}, "age-index-test"); err != nil {
		t.Fatal(err)
	}
	if err := s.User(ctx, 456, "other-owner"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Archive(ctx, 456, []Profile{{"userId": 1024, "name": "foreign-name", "birthday": "2020-01-01"}, {"userId": 3000, "name": "foreign-only", "birthday": "2025-01-01"}}, "age-index-test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.Exec(`INSERT INTO mirror_topics(profile_id,thread_id) VALUES(3000,500)`); err != nil {
		t.Fatal(err)
	}
	first, err := s.ArchiveAgeIndex(ctx, 123, 0)
	if err != nil || first.Total != 25 || first.Offset != 0 || first.NextOffset != 20 || !first.HasMore || len(first.Entities) != 20 {
		t.Fatalf("first page: %+v %v", first, err)
	}
	second, err := s.ArchiveAgeIndex(ctx, 123, first.NextOffset)
	if err != nil || second.Total != 25 || second.Offset != 20 || second.NextOffset != 25 || second.HasMore || len(second.Entities) != 5 {
		t.Fatalf("second page: %+v %v", second, err)
	}
	labels := append(ageIndexLabels(t, first), ageIndexLabels(t, second)...)
	if !strings.Contains(labels[0], "26/165") {
		t.Fatal("long display name hid the index age and height")
	}
	links := append(first.Entities, second.Entities...)
	for i := range 25 {
		id := 1024 - i
		if i == 23 {
			id = 1000
		} else if i == 24 {
			id = 1001
		}
		if !strings.HasSuffix(labels[i], "#"+strconv.Itoa(id)) || links[i].URL != forumTopicURL(c.BackupChatID, int64(id-900)) {
			t.Fatalf("exact DOB order, unknown-last or topic link lost at %d: %q %+v", i, labels[i], links[i])
		}
	}
	if strings.Contains(first.Text+second.Text, "foreign") || strings.Contains(first.Text, "stale-B") || !strings.Contains(first.Text, "26/165") {
		t.Fatal("index exposed another owner, used an old version or lost minimal fields")
	}
	other, err := s.ArchiveAgeIndex(ctx, 456, 0)
	if err != nil || other.Total != 2 || strings.Contains(other.Text, "Name") {
		t.Fatal("other owner index scope changed", err)
	}
	if _, err := s.ArchiveAgeIndex(ctx, 123, -1); err == nil {
		t.Fatal("negative index offset accepted")
	}
	if empty, err := s.ArchiveAgeIndex(ctx, 789, 0); err != nil || empty.Total != 0 || empty.HasMore || len(empty.Entities) != 0 {
		t.Fatal("empty owner index leaked topics", err)
	}
	if beyond, err := s.ArchiveAgeIndex(ctx, 123, 999); err != nil || beyond.Total != 25 || beyond.HasMore || len(beyond.Entities) != 0 {
		t.Fatal("out-of-range index repeated profiles", err)
	}
}

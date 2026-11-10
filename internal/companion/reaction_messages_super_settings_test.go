package companion

import (
	"context"
	"strconv"
	"strings"
	"testing"
)

func TestPositiveSuperRuleSettingsPreserveOffAndMessageCursor(t *testing.T) {
	f, menus := settingsUXFixture(t)
	s := f.worker.Service
	ctx := context.Background()
	if _, err := s.Store.ConfigureReactionMessagePolicy(ctx, 1, ReactionMessagePolicyConfig{Mode: "rotate", Templates: []string{"First", "Second"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.Exec("UPDATE reaction_message_settings SET next_cursor=8 WHERE owner_id=1"); err != nil {
		t.Fatal(err)
	}
	before, err := f.worker.settings(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.HandleSettingsCallback(ctx, 1, "as:"+before.Nonce+":super:23"); err != nil {
		t.Fatal(err)
	}
	p, err := s.Store.ReactionMessagePolicy(ctx, 1)
	if err != nil || p.SuperBeforeAge != 23 || p.NextCursor != 8 {
		t.Fatalf("policy toggle altered templates: %+v %v", p, err)
	}
	after, err := f.worker.settings(ctx, 1)
	if err != nil || after.Mode != "off" || after.RunID != before.RunID {
		t.Fatalf("toggle started automation: %+v %v", after, err)
	}
	if _, err = s.HandleSettingsCallback(ctx, 2, "as:"+before.Nonce+":super:23"); err == nil {
		t.Fatal("cross owner stale menu accepted")
	}
	other, err := s.Store.ReactionMessagePolicy(ctx, 2)
	if err != nil || other.SuperBeforeAge != 0 {
		t.Fatal("other owner's strategy changed", err)
	}
	var count int
	if err = s.Store.DB.QueryRow("SELECT count(*) FROM actions").Scan(&count); err != nil || count != 0 || len(f.requests) > 0 {
		t.Fatal("settings issued a source request or reaction", err)
	}
	if err = s.OpenSettings(ctx, 1); err != nil {
		t.Fatal(err)
	}
	menu := (*menus)[len(*menus)-1]
	if !strings.Contains(menu.Text, "18–22: 🔥 суперлайк · 23+: ❤️ лайк") || !strings.Contains(menu.Text, "выключен") {
		t.Fatalf("unclear strategy in menu: %s", menu.Text)
	}
}

func TestPositiveSuperModesSettingsDoNotStartOrExpandFilters(t *testing.T) {
	f, menus := settingsUXFixture(t)
	s := f.worker.Service
	ctx := context.Background()
	if _, err := s.Store.ConfigureReactionMessagePolicy(ctx, 1, ReactionMessagePolicyConfig{Mode: "first", Templates: []string{"Привет!"}}); err != nil {
		t.Fatal(err)
	}
	beforeFilters, err := f.worker.filters(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, cutoff := range []int{112, 23, 27, 28, 0} {
		before, err := f.worker.settings(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.HandleSettingsCallback(ctx, 1, "as:"+before.Nonce+":super:"+strconv.Itoa(cutoff)); err != nil {
			t.Fatal(err)
		}
		p, err := s.Store.ReactionMessagePolicy(ctx, 1)
		if err != nil || p.SuperBeforeAge != cutoff {
			t.Fatalf("wrong mode: %+v %v", p, err)
		}
		after, err := f.worker.settings(ctx, 1)
		if err != nil || after.Mode != "off" {
			t.Fatalf("setting started auto: %+v %v", after, err)
		}
		filters, err := f.worker.filters(ctx, 1)
		if err != nil || filters != beforeFilters {
			t.Fatal("setting expanded filters", err)
		}
		menu := (*menus)[len(*menus)-1]
		if !strings.Contains(menu.Text, positiveRuleLabel(cutoff)) {
			t.Fatalf("mode missing from menu: %s", menu.Text)
		}
		for _, label := range []string{"Простые", "Супер", "По возрасту"} {
			found := false
			for _, row := range menu.Markup.Rows {
				for _, button := range row {
					if strings.Contains(button.Text, label) {
						found = true
					}
				}
			}
			if !found {
				t.Fatalf("mode button %s missing", label)
			}
		}
	}
	if len(f.requests) > 0 || len(f.sent) > 0 {
		t.Fatal("changing modes performed source HTTP")
	}
}

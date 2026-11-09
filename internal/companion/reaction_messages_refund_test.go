package companion

import (
	"context"
	"testing"
	"time"
)

func refundMessageFixture(t *testing.T, mode string, cursor int64) (*autoFixture, int64) {
	t.Helper()
	f := newAutoFixture(t)
	s := f.worker.Service.Store
	ctx := context.Background()
	f.add(t, 1, 7, 23, false)
	templates := []string{"one", "two", "three"}
	username := ""
	if mode == "birthday" {
		templates = nil
		username = "fixture_user"
	}
	if _, err := s.SetReactionMessagePolicy(ctx, 1, mode, templates, username); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.Exec("UPDATE reaction_message_settings SET next_cursor=? WHERE owner_id=1", cursor); err != nil {
		t.Fatal(err)
	}
	return f, prepareRefundMessage(t, f, 7)
}

func prepareRefundMessage(t *testing.T, f *autoFixture, id int64) int64 {
	t.Helper()
	s := f.worker.Service.Store
	ctx := context.Background()
	p, err := s.Profile(ctx, 1, id)
	if err != nil {
		t.Fatal(err)
	}
	session, err := s.Session(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	action, err := s.ClaimOwnerAction(ctx, 1, id, f.now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.prepareLikeMessage(ctx, 1, action, p, session, nil); err != nil {
		t.Fatal(err)
	}
	return action
}

func TestReactionMessageRefundBirthdayUsesReservedCursor(t *testing.T) {
	const before = int64(169)
	f, action := refundMessageFixture(t, "birthday", before)
	s := f.worker.Service.Store
	ctx := context.Background()
	var reserved, index int64
	if err := s.DB.QueryRow("SELECT reserved_cursor,template_index FROM reaction_message_intents WHERE action_id=?", action).Scan(&reserved, &index); err != nil {
		t.Fatal(err)
	}
	if reserved != before || index == before {
		t.Fatal("fixture did not separate global template index from reservation cursor", reserved, index)
	}
	if err := s.releaseUnsentReaction(ctx, action); err != nil {
		t.Fatal(err)
	}
	p, err := s.ReactionMessagePolicy(ctx, 1)
	if err != nil || p.NextCursor != before {
		t.Fatal("unsent birthday reservation was not refunded", p, err)
	}
	for _, table := range []string{"actions", "reaction_message_intents"} {
		var count int
		if err = s.DB.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatal("unsent row did not cascade atomically", table, count, err)
		}
	}
	if len(f.requests) != 0 {
		t.Fatal("refund performed HTTP")
	}
}

func TestReactionMessageRefundPreservesNewPolicyAndLaterReservations(t *testing.T) {
	for _, replace := range []bool{false, true} {
		name := "later_reservation"
		if replace {
			name = "new_policy_revision"
		}
		t.Run(name, func(t *testing.T) {
			f, old := refundMessageFixture(t, "rotate", 0)
			s := f.worker.Service.Store
			ctx := context.Background()
			if replace {
				if _, err := s.SetReactionMessagePolicy(ctx, 1, "rotate", []string{"replacement one", "replacement two"}); err != nil {
					t.Fatal(err)
				}
			}
			f.add(t, 1, 8, 23, false)
			later := prepareRefundMessage(t, f, 8)
			before, err := s.ReactionMessagePolicy(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			if err = s.releaseUnsentReaction(ctx, old); err != nil {
				t.Fatal(err)
			}
			after, err := s.ReactionMessagePolicy(ctx, 1)
			if err != nil || after.NextCursor != before.NextCursor || after.Revision != before.Revision {
				t.Fatal("refund rewound another reservation or a replaced policy", before, after, err)
			}
			var count int
			if err = s.DB.QueryRow("SELECT count(*) FROM reaction_message_intents WHERE action_id=?", later).Scan(&count); err != nil || count != 1 {
				t.Fatal("refund removed a later intent", count, err)
			}
		})
	}
}

func TestReactionMessageRefundNeverReleasesUnconfirmedPost(t *testing.T) {
	f, action := refundMessageFixture(t, "rotate", 0)
	s := f.worker.Service.Store
	ctx := context.Background()
	if err := s.finishReactionAction(ctx, action, "unknown", []byte(`{"fixture":"response lost"}`), time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := s.releaseUnsentReaction(ctx, action); err != nil {
		t.Fatal(err)
	}
	p, err := s.ReactionMessagePolicy(ctx, 1)
	if err != nil || p.NextCursor != 1 {
		t.Fatal("unconfirmed POST refunded its rotation", p, err)
	}
	var state string
	if err = s.DB.QueryRow("SELECT a.state FROM actions a JOIN reaction_message_intents i ON i.action_id=a.id WHERE a.id=?", action).Scan(&state); err != nil || state != "unknown" {
		t.Fatal("unconfirmed action or intent was deleted", state, err)
	}
}

func TestReactionMessageRefundDeletionFailureRollsBackCursor(t *testing.T) {
	f, action := refundMessageFixture(t, "rotate", 0)
	s := f.worker.Service.Store
	ctx := context.Background()
	if _, err := s.DB.Exec(`CREATE TRIGGER refund_delete_failure BEFORE DELETE ON actions BEGIN SELECT RAISE(ABORT,'fixture delete failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := s.releaseUnsentReaction(ctx, action); err == nil {
		t.Fatal("fixture deletion failure ignored")
	}
	p, err := s.ReactionMessagePolicy(ctx, 1)
	if err != nil || p.NextCursor != 1 {
		t.Fatal("failed deletion committed a partial cursor refund", p, err)
	}
	var count int
	if err = s.DB.QueryRow("SELECT count(*) FROM actions a JOIN reaction_message_intents i ON i.action_id=a.id WHERE a.id=?", action).Scan(&count); err != nil || count != 1 {
		t.Fatal("failed transaction lost action or intent", count, err)
	}
}

func TestReactionMessageRefundWorksForLegacyActionWithoutTables(t *testing.T) {
	f := newAutoFixture(t)
	s := f.worker.Service.Store
	ctx := context.Background()
	f.add(t, 1, 7, 23, false)
	action, err := s.ClaimOwnerAction(ctx, 1, 7, f.now)
	if err != nil {
		t.Fatal(err)
	}
	var tables int
	if err = s.DB.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('reaction_message_settings','reaction_message_intents')").Scan(&tables); err != nil || tables != 0 {
		t.Fatal("fixture unexpectedly initialized message tables", tables, err)
	}
	if err = s.releaseUnsentReaction(ctx, action); err != nil {
		t.Fatal("legacy unsent action could not be released", err)
	}
	var actions int
	if err = s.DB.QueryRow("SELECT count(*) FROM actions WHERE id=?", action).Scan(&actions); err != nil || actions != 0 {
		t.Fatal("legacy action not removed", actions, err)
	}
}

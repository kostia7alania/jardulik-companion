package companion

import (
	"context"
	"strings"
	"testing"
)

func TestOpeningPostsRefreshKeepsAcceptedSendWithoutLocalReceipt(t *testing.T) {
	for _, change := range []string{"policy", "source_unavailable"} {
		t.Run(change, func(t *testing.T) {
			f := newOpeningPostsFixture(t)
			ctx := context.Background()
			f.addDraft(t, 10, "Ира", "Ира, привет! Какая музыка? @fixture_user")
			f.step(t) // Create the topic.
			f.step(t) // Publish the formula before the recipient.
			if _, err := f.s.Store.DB.Exec(`CREATE TRIGGER opening_receipt_failure BEFORE UPDATE ON opening_audit_posts
 WHEN OLD.item_key='draft:10' AND OLD.state='sending' AND NEW.state='sent'
 BEGIN SELECT RAISE(FAIL,'receipt checkpoint unavailable'); END`); err != nil {
				t.Fatal(err)
			}
			if worked, err := f.s.deliverOpeningPost(ctx); !worked || err == nil || !strings.Contains(err.Error(), "receipt checkpoint unavailable") {
				t.Fatal("accepted send did not reproduce a lost local receipt", worked, err)
			}
			if len(f.methods) != 3 || f.methods[2] != "sendMessage" {
				t.Fatal("the recipient was not accepted once by Telegram", f.methods)
			}
			const snapshot = "SELECT state||':'||message_id||':'||source_sha||':'||desired_sha FROM opening_audit_posts WHERE item_key='draft:10'"
			before := f.scalar(t, snapshot)
			if !strings.HasPrefix(before, "sending:0:") {
				t.Fatal("failed checkpoint did not retain an unresolved send", before)
			}
			if _, err := f.s.Store.DB.Exec("DROP TRIGGER opening_receipt_failure"); err != nil {
				t.Fatal(err)
			}
			expected, err := f.s.Store.Session(ctx, 123)
			if err != nil {
				t.Fatal(err)
			}
			if change == "source_unavailable" {
				if _, err = f.s.Store.DB.Exec("DELETE FROM mirror_topics WHERE profile_id=10"); err != nil {
					t.Fatal(err)
				}
				f.refresh(t)
				if err = f.s.refreshOpeningPosts(ctx, expected); err != nil {
					t.Fatal(err)
				}
				if after := f.scalar(t, snapshot); after != before {
					t.Fatal("missing source erased the unresolved send", before, after)
				}
				if _, err = f.s.Store.DB.Exec("INSERT INTO mirror_topics(profile_id,thread_id) VALUES(10,110)"); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = f.s.Store.ConfigureReactionMessagePolicy(ctx, 123, ReactionMessagePolicyConfig{Mode: "birthday", TelegramUsername: "fixture_user", PreferDrafts: true, IronMix: false}); err != nil {
				t.Fatal(err)
			}
			f.refresh(t)
			if err = f.s.refreshOpeningPosts(ctx, expected); err != nil {
				t.Fatal(err)
			}
			if after := f.scalar(t, snapshot); after != before {
				t.Fatal("changed source reset the unresolved send", before, after)
			}
			// The formula has a known ID, so its changed policy can still be edited.
			f.step(t)
			if f.methods[len(f.methods)-1] != "editMessageText" {
				t.Fatal("known formula edit was blocked", f.methods)
			}
			if worked, err := f.s.deliverOpeningPost(ctx); worked || err != nil || len(f.methods) != 4 {
				t.Fatal("unresolved recipient was blindly resent", worked, err, f.methods)
			}
			if n := watchCount(t, f.s.Store, "actions"); n != 0 {
				t.Fatal("audit recovery sent a Jardulik reaction", n)
			}
		})
	}
}

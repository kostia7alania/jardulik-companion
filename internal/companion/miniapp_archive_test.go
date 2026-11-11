package companion

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMiniAppArchiveViewerPinsOwnerVersion(t *testing.T) {
	f := newMiniAppFixture(t)
	ctx := context.Background()
	s := f.service.Store
	oldIDs, err := s.ArchiveViewed(ctx, 1, []Profile{{"userId": 7, "description": "Старое описание", "number": json.Number("9007199254740993"), "decimal": json.Number("1.00")}}, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	old, err := s.Version(ctx, 1, oldIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	latestIDs, err := s.ArchiveViewed(ctx, 1, []Profile{{"userId": 7, "description": "Новое описание"}}, "fixture")
	if err != nil || len(latestIDs) != 1 || latestIDs[0] == old.VersionID {
		t.Fatalf("new version not created: %v %v", latestIDs, err)
	}
	data := []byte("archived fixture image")
	sha := digest(data)
	dir := filepath.Join(f.service.Config.DataDir, "media")
	if err = os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, sha), data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.Exec("INSERT INTO media(sha,path,mime,size) VALUES(?,?,?,?)", sha, "fixture", "image/jpeg", len(data)); err != nil {
		t.Fatal(err)
	}
	addMedia := func(version int64) string {
		t.Helper()
		result, err := s.DB.Exec("INSERT INTO media_jobs(owner_id,version_id,role,position,url,state,sha) VALUES(1,?,'photos',0,'https://fixture.invalid/image','complete',?)", version, sha)
		if err != nil {
			t.Fatal(err)
		}
		id, err := result.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("/miniapp/media/%d", id)
	}
	oldMedia, newMedia := addMedia(old.VersionID), addMedia(latestIDs[0])
	otherProfile, err := s.Profile(ctx, 1, 8)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.User(ctx, 2, "other"); err != nil {
		t.Fatal(err)
	}
	foreignIDs, err := s.ArchiveViewed(ctx, 2, []Profile{{"userId": 7, "description": "Другой владелец"}}, "fixture")
	if err != nil {
		t.Fatal(err)
	}

	launch := func(path string, owner, version int64) (int, string) {
		t.Helper()
		raw, _ := json.Marshal(map[string]any{"initData": miniAppLaunch(f.service.Config.Token, owner, f.h.now()), "targetUserId": 7, "versionId": version})
		w := f.request("POST", path, string(raw), "")
		return w.Code, w.Body.String()
	}
	for _, rejected := range []struct {
		path    string
		owner   int64
		version int64
		status  int
	}{
		{"/miniapp/auth", 2, old.VersionID, http.StatusUnauthorized},
		{"/miniapp/auth", 1, otherProfile.VersionID, http.StatusNotFound},
		{"/miniapp/auth", 1, foreignIDs[0], http.StatusNotFound},
		{"/miniapp/auth", 1, 0, http.StatusBadRequest},
		{"/miniapp/api/appuser/login/telegram", 1, old.VersionID, http.StatusBadRequest},
	} {
		status, body := launch(rejected.path, rejected.owner, rejected.version)
		if status != rejected.status {
			t.Fatalf("rejected archive launch HTTP %d: %s", status, body)
		}
	}
	if len(f.calls) != 0 {
		t.Fatalf("invalid archive launch contacted source: %v", f.calls)
	}
	w := f.request("GET", "/miniapp/profile?id=7", "", f.login(t, "/miniapp/auth"))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "Новое описание") || len(f.calls) != 1 {
		t.Fatalf("legacy profile-only viewer stopped resolving the latest version: HTTP%d %s", w.Code, w.Body.String())
	}
	f.calls = nil

	checkArchive := func() {
		t.Helper()
		status, body := launch("/miniapp/auth", 1, old.VersionID)
		var auth struct{ Token string }
		if status != http.StatusOK || json.Unmarshal([]byte(body), &auth) != nil || auth.Token == "" {
			t.Fatalf("exact archive launch HTTP %d: %s", status, body)
		}
		w := f.request("GET", "/miniapp/profile?id=7", "", auth.Token)
		var archived struct {
			VersionID              int64
			RawJSON, FormattedText string
		}
		if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &archived) != nil || archived.VersionID != old.VersionID ||
			archived.RawJSON != old.RawJSON || !strings.Contains(archived.FormattedText, "Старое описание") {
			t.Fatalf("archive view did not match the selected version: HTTP%d %s", w.Code, w.Body.String())
		}
		if w = f.request("GET", oldMedia, "", auth.Token); w.Code != http.StatusOK || w.Body.String() != string(data) {
			t.Fatalf("local archive media unavailable: HTTP%d", w.Code)
		}
		if w = f.request("GET", newMedia, "", auth.Token); w.Code != http.StatusNotFound {
			t.Fatalf("archive token granted another version's media: HTTP%d", w.Code)
		}
		if w = f.request("GET", "/miniapp/api/user/getMe", "", auth.Token); w.Code != http.StatusForbidden || len(f.calls) != 0 {
			t.Fatalf("archive token can reach live API: HTTP%d %v", w.Code, f.calls)
		}
	}
	checkArchive()
	s.Pause(ctx, 1, true)
	checkArchive()
	s.Pause(ctx, 1, false)
	if err = s.Disconnect(ctx, 1); err != nil {
		t.Fatal(err)
	}
	checkArchive()
	raw, _ := json.Marshal(map[string]any{"initData": miniAppLaunch(f.service.Config.Token, 1, f.h.now()), "targetUserId": 7})
	if w = f.request("POST", "/miniapp/auth", string(raw), ""); w.Code != http.StatusConflict || len(f.calls) != 0 {
		t.Fatalf("legacy launch worked without source session: HTTP%d %v", w.Code, f.calls)
	}
}

func TestMiniAppArchiveCallbackUsesExactOwnerZIPReceipt(t *testing.T) {
	s, p := miniAppTestCardFixture(t, 0)
	s.Config.BackupChatID, s.Config.BackupThreadID = -10042, 1
	const thread, zip = 8, 17
	ctx := context.Background()
	if _, err := s.Store.DB.Exec(archiveSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.Exec("INSERT INTO archive_versions(version_id,owner_id,profile_id,fingerprint,state,thread_id,source_sha) VALUES(?,1,?,'fixture','uncertain',?,'fixture')", p.VersionID, p.ID, thread); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.DB.Exec(`INSERT INTO archive_receipts(source_sha,kind,filename,byte_size,chat_id,thread_id,message_id,file_id,file_unique_id,returned_file_size)
 VALUES('fixture','profile_version','source.zip',1,?,?,?,'file','unique',1)`, s.Config.BackupChatID, thread, zip); err != nil {
		t.Fatal(err)
	}

	a := &Archiver{Config: s.Config}
	action := fmt.Sprintf("%s%d", archiveMiniAppAction, p.VersionID)
	ownerMarkup, _ := json.Marshal(a.archiveProfileKeyboard(1, p.VersionID))
	foreignMarkup, _ := json.Marshal(a.archiveProfileKeyboard(2, p.VersionID))
	if !strings.Contains(string(ownerMarkup), action) || strings.Contains(string(foreignMarkup), action) {
		t.Fatal("archive control owner binding changed")
	}

	sends, answers := 0, 0
	s.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch filepath.Base(r.URL.Path) {
		case "answerCallbackQuery":
			answers++
			fmt.Fprint(w, `{"ok":true,"result":true}`)
		case "sendMessage":
			sends++
			var payload struct {
				ChatID   int64 `json:"chat_id"`
				ThreadID int64 `json:"message_thread_id"`
				Markup   struct {
					Rows [][]struct {
						WebApp struct{ URL string } `json:"web_app"`
					} `json:"inline_keyboard"`
				} `json:"reply_markup"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload.ChatID != 1 || payload.ThreadID != 0 ||
				len(payload.Markup.Rows) != 1 || len(payload.Markup.Rows[0]) != 1 ||
				payload.Markup.Rows[0][0].WebApp.URL != fmt.Sprintf("https://companion.example/viewer?id=10&versionId=%d", p.VersionID) {
				t.Fatalf("wrong private archive link: %+v %v", payload, err)
			}
			fmt.Fprint(w, `{"ok":true,"result":{"message_id":19,"chat":{"id":1,"type":"private"}}}`)
		default:
			t.Fatalf("unexpected Bot API call: %s", r.URL.Path)
		}
	})}

	c := Callback{
		ID: "archive-fixture", Data: action, From: TGUser{ID: 1},
		Message: Message{ID: zip, ThreadID: thread, Chat: TGChat{ID: s.Config.BackupChatID, Type: "supergroup"}},
	}
	for _, change := range []func(*Callback){
		func(c *Callback) { c.From.ID = 2 },
		func(c *Callback) { c.Message.Chat.ID-- },
		func(c *Callback) { c.Message.ThreadID++ },
		func(c *Callback) { c.Message.ID++ },
	} {
		other := c
		change(&other)
		s.Handle(ctx, Update{Callback: &other})
	}
	if sends != 0 {
		t.Fatal("an unrelated callback sent a private link")
	}
	s.Handle(ctx, Update{Callback: &c})
	if sends != 1 || answers != 3 {
		t.Fatalf("unexpected archive callback requests: sends=%d, answers=%d", sends, answers)
	}
}

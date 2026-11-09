package companion

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestConnectCommandChecksIdentityOnceBeforeSuccessMenu(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			store, cfg, client, raw := atomicLoginFixture(t, true)
			before := loginBinding(t, store)
			service := &Service{Config: cfg, Store: store, Client: client, TG: NewTelegram(cfg), Media: NewMediaManager(store, cfg)}
			loginCalls, accountCalls := 0, 0
			client.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/appuser/login/telegram":
					loginCalls++
					fmt.Fprint(w, validAtomicLogin)
				case "/user/getMe":
					accountCalls++
					if fail {
						w.WriteHeader(http.StatusServiceUnavailable)
						fmt.Fprint(w, `{}`)
						return
					}
					fmt.Fprint(w, `{"userId":5,"type":1,"freezeState":false}`)
				default:
					t.Fatal("unexpected source request", r.URL.Path)
				}
			})}
			var sent []uxMessage
			service.TG.HTTP.Transport = handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if filepath.Base(r.URL.Path) == "deleteMessage" {
					fmt.Fprint(w, `{"ok":true,"result":true}`)
					return
				}
				if filepath.Base(r.URL.Path) != "sendMessage" {
					t.Fatal("unexpected Telegram operation", r.URL.Path)
				}
				var message uxMessage
				if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
					t.Fatal(err)
				}
				sent = append(sent, message)
				fmt.Fprint(w, `{"ok":true,"result":{"message_id":71,"chat":{"id":1,"type":"private"}}}`)
			})}
			service.Handle(context.Background(), Update{Message: &Message{ID: 20, From: TGUser{ID: 1}, Chat: TGChat{ID: 1, Type: "private"}, Text: "/connect " + raw}})
			if loginCalls != 1 || accountCalls != 1 {
				t.Fatalf("connect performed duplicate requests: login=%d account=%d", loginCalls, accountCalls)
			}
			if len(sent) != 1 || strings.Contains(sent[0].Text, raw) {
				t.Fatal("connect result missing or leaked credential")
			}
			if fail {
				if !reflect.DeepEqual(before, loginBinding(t, store)) || loginSuccessEvents(t, store) != 0 || !strings.Contains(sent[0].Text, "Не удалось подключить") {
					t.Fatal("failed identity check changed session or reported success")
				}
			} else {
				if loginSuccessEvents(t, store) != 1 || !strings.Contains(sent[0].Text, "Jardulik подключён") {
					t.Fatal("verified login not acknowledged")
				}
				var next, settings bool
				for _, row := range sent[0].Markup.Rows {
					for _, b := range row {
						next = next || b.Data == "next"
						settings = settings || b.Data == "settings"
					}
				}
				if !next || !settings {
					t.Fatal("connect success has no next actions")
				}
			}
		})
	}
}

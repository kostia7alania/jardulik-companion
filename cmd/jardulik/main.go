package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/kostia7alania/jardulik-companion/internal/companion"
)

func run() error {
	command := "serve"
	if len(os.Args) > 1 {
		command = os.Args[1]
	}
	if command == "version" {
		fmt.Println(companion.Version)
		return nil
	}
	c, err := companion.LoadConfig()
	if err != nil {
		return err
	}
	if command == "restore" {
		if len(os.Args) != 4 {
			return errors.New("usage: jardulik restore <encrypted-archive> <new-empty-directory>")
		}
		return companion.RestoreArchive(os.Args[2], os.Args[3], c.BackupKey)
	}
	if command == "restore-media" {
		if len(os.Args) != 3 {
			return errors.New("usage: jardulik restore-media <restored-directory>")
		}
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()
		result, err := companion.RestoreMedia(ctx, companion.NewTelegram(c), os.Args[2])
		_ = json.NewEncoder(os.Stdout).Encode(result)
		return err
	}
	s, err := companion.NewService(c)
	if err != nil {
		return err
	}
	defer s.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	owner := c.OwnerID
	if v := os.Getenv("TENANT_ID"); v != "" {
		owner, err = strconv.ParseInt(v, 10, 64)
		if err != nil || owner <= 0 {
			return errors.New("invalid TENANT_ID")
		}
	}
	switch command {
	case "serve":
		return s.Run(ctx)
	case "status":
		if err = s.Store.Check(ctx); err != nil {
			return err
		}
		out := map[string]any{"version": companion.Version, "sqlite": "ok", "reactions_enabled": c.AllowReactions, "metrics": s.Store.AdminCounts(ctx), "backup": s.Archiver.BackupStatus(ctx)}
		if session, e := s.Store.Session(ctx, owner); e == nil {
			out["session"] = map[string]any{"tenant": owner, "user_id": session.UserID, "state": session.State, "expires_at": session.Expires, "account": json.RawMessage(session.Account)}
		}
		return json.NewEncoder(os.Stdout).Encode(out)
	case "connect":
		b, err := io.ReadAll(io.LimitReader(os.Stdin, 32*1024+1))
		if err != nil {
			return err
		}
		raw, err := companion.ParseInitData(strings.TrimSpace(string(b)))
		if err != nil {
			return err
		}
		if err = s.Store.User(ctx, owner, "cli"); err != nil {
			return err
		}
		if err = s.Client.Login(ctx, owner, raw); err != nil {
			return err
		}
		account, err := s.Client.Me(ctx, owner)
		if err != nil {
			return err
		}
		out := map[string]any{"connected": true, "tenant": owner, "user_id": account.ID(), "type": account["type"], "order_end_date": account["orderEndDate"]}
		return json.NewEncoder(os.Stdout).Encode(out)
	case "fetch":
		n, err := s.Fetch(ctx, owner)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"profiles_received": n, "archive": s.Store.Counts(ctx, owner), "reactions_sent": 0})
	case "account":
		p, err := s.Client.Me(ctx, owner)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"user_id": p.ID(), "type": p["type"], "order_end_date": p["orderEndDate"], "freeze_state": p["freezeState"], "fire": p["fire"], "clean": p["clean"]})
	case "backup":
		if err = s.Archiver.RequestBackup(ctx); err != nil {
			return err
		}
		fmt.Println("Backup requested; delivery is confirmed separately in /status and the private backup topic.")
		return nil
	case "repair-posts":
		if err = s.Archiver.RequestProfileRepair(ctx); err != nil {
			return err
		}
		if err = s.RequestPrivateCardRepair(ctx); err != nil {
			return err
		}
		fmt.Println("Profile post repair requested. Completion is recorded separately; original ZIP documents are retained.")
		return nil
	case "repair-zips":
		return s.Archiver.RequestZIPRepair(ctx)
	case "reconcile-albums":
		if err = s.Archiver.ReconcileCaptionedAlbums(ctx); err != nil {
			return err
		}
		return s.RefreshCardCaptions(ctx)
	default:
		return errors.New("commands: serve, connect (URL on stdin), account, fetch, status, backup, restore, restore-media, repair-posts (service stopped), version")
	}
}
func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	if err := run(); err != nil {
		slog.Error("command failed", "error", err.Error())
		os.Exit(1)
	}
}

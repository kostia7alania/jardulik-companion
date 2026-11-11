package main

import (
	"context"
	"encoding/base64"
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
	if (command == "serve-bot-parser" || command == "serve-poster" || command == "serve-compatibility" || command == "serve-miniapp") && len(os.Args) != 2 {
		return errors.New("usage: jardulik " + command)
	}
	if command == "version" {
		fmt.Println(companion.Version)
		return nil
	}
	if command == "restore" {
		if len(os.Args) != 4 {
			return errors.New("usage: jardulik restore <encrypted-archive> <new-empty-directory>")
		}
		key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(os.Getenv("BACKUP_KEY")))
		if err != nil || len(key) != 32 {
			return errors.New("BACKUP_KEY must be base64-encoded 32 bytes")
		}
		return companion.RestoreArchive(os.Args[2], os.Args[3], key)
	}
	c, err := companion.LoadConfig()
	if err != nil {
		return err
	}
	roles := map[string]companion.RuntimeRole{"serve": companion.RuntimeAll, "serve-bot-parser": companion.RuntimeBotParser, "serve-poster": companion.RuntimePoster, "serve-compatibility": companion.RuntimeCompatibility, "serve-miniapp": companion.RuntimeMiniApp}
	if role, serving := roles[command]; serving {
		if logErr := companion.InstallOperationsLogger(c, role); logErr != nil {
			slog.Warn("operations log unavailable", "kind", fmt.Sprintf("%T", logErr))
		}
	}
	if command == "restore-telegram" {
		if len(os.Args) != 4 {
			return errors.New("usage: jardulik restore-telegram <receipt.json> <new-empty-directory>")
		}
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()
		work, tempErr := os.MkdirTemp("", "jardulik-telegram-restore-")
		if tempErr != nil {
			return tempErr
		}
		defer os.RemoveAll(work)
		input := work + "/full.zip.enc"
		if err = companion.DownloadFullBackup(ctx, c, os.Args[2], input); err != nil {
			return err
		}
		return companion.RestoreArchive(input, os.Args[3], c.BackupKey)
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
	case "serve-bot-parser":
		return s.RunBotParser(ctx)
	case "serve-poster":
		return s.RunPoster(ctx)
	case "serve-compatibility":
		return s.RunCompatibility(ctx)
	case "serve-miniapp":
		return s.RunMiniApp(ctx)
	case "miniapp-test-card":
		if len(os.Args) != 3 {
			return errors.New("usage: jardulik miniapp-test-card <userId>")
		}
		profile, parseErr := strconv.ParseInt(os.Args[2], 10, 64)
		if parseErr != nil || profile <= 0 {
			return errors.New("invalid profile userId")
		}
		result, sendErr := s.SendMiniAppTestCard(ctx, profile)
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"userId": profile, "message_id": result.Message.ID, "already_sent": result.AlreadySent})
		return sendErr
	case "sync-status":
		if len(os.Args) != 2 {
			return errors.New("usage: jardulik sync-status")
		}
		status, statusErr := s.SyncStatus(ctx, owner)
		if statusErr != nil {
			return statusErr
		}
		return json.NewEncoder(os.Stdout).Encode(status)
	case "operations-status":
		if len(os.Args) != 2 {
			return errors.New("usage: jardulik operations-status")
		}
		status, statusErr := s.OperationsStatus(ctx)
		if statusErr != nil {
			return statusErr
		}
		fmt.Println(status)
		return nil
	case "compatibility-status":
		if len(os.Args) != 2 {
			return errors.New("usage: jardulik compatibility-status")
		}
		status, statusErr := s.Store.CompatibilityStatus(ctx, owner)
		if statusErr != nil {
			return statusErr
		}
		return json.NewEncoder(os.Stdout).Encode(status)
	case "reaction-draft-import":
		if len(os.Args) != 2 {
			return errors.New("usage: jardulik reaction-draft-import < reviewed.json")
		}
		return importReactionMessageDrafts(ctx, s, owner, os.Stdin, os.Stdout)
	case "reaction-hold-resolve":
		if len(os.Args) != 2 {
			return errors.New("usage: jardulik reaction-hold-resolve < resolution.json")
		}
		return resolveReactionMessageHold(ctx, s, owner, os.Stdin, os.Stdout)
	case "reaction-draft-status":
		if len(os.Args) != 2 {
			return errors.New("usage: jardulik reaction-draft-status")
		}
		status, err := s.Store.ReactionMessageDraftStatus(ctx, owner)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(status)
	case "reaction-message-status":
		if len(os.Args) != 2 {
			return errors.New("usage: jardulik reaction-message-status")
		}
		policy, policyErr := s.Store.ReactionMessagePolicy(ctx, owner)
		if policyErr != nil {
			return policyErr
		}
		return json.NewEncoder(os.Stdout).Encode(policy)
	case "reaction-message-set":
		if len(os.Args) != 2 {
			return errors.New("usage: jardulik reaction-message-set < policy.json")
		}
		return setReactionMessagePolicy(ctx, s, owner, os.Stdin, os.Stdout)
	case "status":
		if err = s.Store.Check(ctx); err != nil {
			return err
		}
		out := map[string]any{"version": companion.Version, "sqlite": "ok", "reactions_enabled": c.AllowReactions, "metrics": s.Store.AdminCounts(ctx), "backup": s.Archiver.BackupStatus(ctx)}
		if session, e := s.Store.Session(ctx, owner); e == nil {
			out["session"] = map[string]any{"tenant": owner, "user_id": session.UserID, "state": session.State, "expires_at": session.Expires, "account": json.RawMessage(session.Account)}
		}
		return json.NewEncoder(os.Stdout).Encode(out)
	case "import-analysis":
		if len(os.Args) != 2 {
			return errors.New("usage: jardulik import-analysis < capture.json")
		}
		return importAnalysisCapture(ctx, s, owner, os.Stdin, os.Stdout)
	case "import-contact":
		if len(os.Args) != 2 {
			return errors.New("usage: jardulik import-contact < evidence.json")
		}
		return importContactEvidence(ctx, s, owner, os.Stdin, os.Stdout)
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
		account, err := s.Client.LoginAccount(ctx, owner, raw)
		if err != nil {
			return err
		}
		out := map[string]any{"connected": true, "tenant": owner, "user_id": account.ID(), "type": account["type"], "order_end_date": account["orderEndDate"]}
		return json.NewEncoder(os.Stdout).Encode(out)
	case "crawl-request":
		includeOthers, includeLocations := false, false
		for _, arg := range os.Args[2:] {
			switch {
			case arg == "--include-others" && !includeOthers:
				includeOthers = true
			case arg == "--include-locations" && !includeLocations:
				includeLocations = true
			default:
				return errors.New("usage: crawl-request [--include-others] [--include-locations]")
			}
		}
		job, err := s.Crawler.Request(ctx, owner, includeOthers, includeLocations)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"crawl": job, "reactions_sent": 0})
	case "crawl-status":
		if len(os.Args) != 2 {
			return errors.New("usage: crawl-status")
		}
		job, err := s.Store.CrawlStatus(ctx, owner)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"crawl": job})
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
	case "backup-full":
		if len(os.Args) > 3 || (len(os.Args) == 3 && os.Args[2] != "--if-changed") {
			return errors.New("usage: jardulik backup-full [--if-changed] (build locally; no upload)")
		}
		result, buildErr := s.Archiver.BuildFullBackup(ctx, len(os.Args) == 3)
		_ = json.NewEncoder(os.Stdout).Encode(result)
		return buildErr
	case "backup-send-once":
		if len(os.Args) != 2 {
			return errors.New("usage: jardulik backup-send-once (requires explicit transfer activation)")
		}
		handled, transferErr := s.Archiver.ProcessFullBackupOnce(ctx)
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"handled": handled, "backup": s.Archiver.BackupStatus(ctx)})
		return transferErr
	case "backup-daily":
		if len(os.Args) != 2 {
			return errors.New("usage: jardulik backup-daily")
		}
		if err = s.Archiver.RequestFullBackup(ctx, true); err != nil {
			return err
		}
		fmt.Println("Daily full backup requested; unchanged sources are skipped.")
		return nil
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
	case "refresh-captions":
		cardsOnly := len(os.Args) == 3 && os.Args[2] == "--cards-only"
		if len(os.Args) > 2 && !cardsOnly {
			return errors.New("usage: refresh-captions [--cards-only]")
		}
		result, err := s.RefreshCaptions(ctx, cardsOnly)
		_ = json.NewEncoder(os.Stdout).Encode(result)
		return err
	case "refresh-archive-viewer-control":
		if len(os.Args) != 3 {
			return errors.New("usage: jardulik refresh-archive-viewer-control <versionId>")
		}
		version, parseErr := strconv.ParseInt(os.Args[2], 10, 64)
		if parseErr != nil || version <= 0 {
			return errors.New("invalid archive versionId")
		}
		if owner != c.OwnerID {
			return errors.New("archive viewer controls require the primary owner")
		}
		result, refreshErr := s.RefreshArchiveViewerControl(ctx, version)
		if refreshErr != nil {
			return refreshErr
		}
		return json.NewEncoder(os.Stdout).Encode(result)
	case "refresh-profile-caption":
		if len(os.Args) != 3 {
			return errors.New("usage: jardulik refresh-profile-caption <userId>")
		}
		profile, parseErr := strconv.ParseInt(os.Args[2], 10, 64)
		if parseErr != nil || profile <= 0 {
			return errors.New("invalid profile userId")
		}
		edited, refreshErr := s.RefreshProfileCaption(ctx, profile)
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"userId": profile, "edited": edited})
		return refreshErr
	case "reconcile-albums":
		if err = s.Archiver.ReconcileCaptionedAlbums(ctx); err != nil {
			return err
		}
		return s.RefreshCardCaptions(ctx)
	default:
		return errors.New("commands: serve, serve-bot-parser, serve-poster, serve-compatibility, serve-miniapp, miniapp-test-card, sync-status, operations-status, compatibility-status, reaction-message-status, reaction-message-set (JSON on stdin), reaction-draft-import, reaction-draft-status, reaction-hold-resolve, connect (URL on stdin), account, fetch, import-analysis, crawl-request, crawl-status, status, backup, backup-daily, backup-send-once, backup-full, restore, restore-telegram, restore-media, repair-posts, repair-zips, reconcile-albums, refresh-captions, refresh-profile-caption, refresh-archive-viewer-control, version")
	}
}
func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	if err := run(); err != nil {
		slog.Error("command failed", "error", err)
		os.Exit(1)
	}
}

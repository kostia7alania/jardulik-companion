package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

const reactionMessageSchema = `
CREATE TABLE IF NOT EXISTS reaction_message_settings (
 owner_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
 mode TEXT NOT NULL DEFAULT 'off' CHECK(mode IN ('off','first','rotate','birthday')),
 templates_json TEXT NOT NULL DEFAULT '[]', telegram_username TEXT NOT NULL DEFAULT '', prefer_drafts INTEGER NOT NULL DEFAULT 0 CHECK(prefer_drafts IN (0,1)), next_cursor INTEGER NOT NULL DEFAULT 0 CHECK(next_cursor>=0),
 revision INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS reaction_message_intents (
 action_id INTEGER PRIMARY KEY REFERENCES actions(id) ON DELETE CASCADE,
 owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 upstream_id INTEGER NOT NULL, profile_id INTEGER NOT NULL, generation TEXT NOT NULL,
 message TEXT NOT NULL, request_body BLOB NOT NULL, message_mode TEXT NOT NULL,
 policy_revision INTEGER NOT NULL, template_index INTEGER NOT NULL, reserved_cursor INTEGER NOT NULL DEFAULT -1,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);`

var reactionMessageUsernamePattern = regexp.MustCompile(`^[A-Za-z0-9_]{5,32}$`)

var ErrInvalidReactionMessage = errors.New("сообщение должно содержать не более 500 UTF-16 символов и корректный UTF-8")
var ErrReactionMessageTargetAge = errors.New("для сообщения при лайке нужен подтверждённый возраст 18+")

type ReactionMessagePolicy struct {
	OwnerID          int64    `json:"owner_id"`
	Mode             string   `json:"mode"`
	Templates        []string `json:"templates"`
	TelegramUsername string   `json:"telegram_username"`
	PreferDrafts     bool     `json:"prefer_drafts"`
	Selection        string   `json:"selection"`
	NextCursor       int64    `json:"next_cursor"`
	Revision         int64    `json:"revision"`
}

func validateReactionMessage(message string) error {
	if !utf8.ValidString(message) || textUnits(message) > 500 {
		return ErrInvalidReactionMessage
	}
	return nil
}

func validateReactionMessagePolicy(mode string, templates []string, telegramUsername string) error {
	if mode != "off" && mode != "first" && mode != "rotate" && mode != "birthday" {
		return errors.New("режим сообщения: off, first, rotate или birthday")
	}
	if len(templates) > 3 || (mode == "first" || mode == "rotate") && len(templates) == 0 {
		return errors.New("укажи от одного до трёх шаблонов")
	}
	if telegramUsername != "" && !reactionMessageUsernamePattern.MatchString(telegramUsername) || mode == "birthday" && telegramUsername == "" {
		return errors.New("укажи Telegram username без @: 5–32 латинских букв, цифр или _")
	}
	for _, message := range templates {
		if err := validateReactionMessage(message); err != nil {
			return err
		}
		if strings.TrimSpace(message) == "" {
			return errors.New("шаблон сообщения пуст")
		}
	}
	return nil
}

func (s *Store) initReactionMessages(ctx context.Context) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='reaction_message_settings'"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, reactionMessageSchema+reactionMessageDraftTable); err != nil {
		return err
	}
	var present int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM pragma_table_info('reaction_message_settings') WHERE name='prefer_drafts'").Scan(&present); err != nil {
		return err
	}
	if present == 0 {
		if _, err = tx.ExecContext(ctx, "ALTER TABLE reaction_message_settings ADD COLUMN prefer_drafts INTEGER NOT NULL DEFAULT 0 CHECK(prefer_drafts IN (0,1))"); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func scanReactionMessagePolicy(row interface{ Scan(...any) error }, owner int64) (ReactionMessagePolicy, error) {
	p := ReactionMessagePolicy{OwnerID: owner, Mode: "off", Templates: []string{}, Selection: "off"}
	var raw string
	err := row.Scan(&p.Mode, &raw, &p.TelegramUsername, &p.PreferDrafts, &p.NextCursor, &p.Revision)
	if errors.Is(err, sql.ErrNoRows) {
		return p, nil
	}
	if err != nil {
		return p, err
	}
	if err = json.Unmarshal([]byte(raw), &p.Templates); err != nil {
		return p, errors.New("не удалось прочитать шаблоны сообщений")
	}
	if p.Templates == nil {
		p.Templates = []string{}
	}
	if err = validateReactionMessagePolicy(p.Mode, p.Templates, p.TelegramUsername); err != nil {
		return p, err
	}
	if p.PreferDrafts && p.Mode != "birthday" {
		return p, errors.New("reviewed drafts require birthday fallback mode")
	}
	p.Selection = p.Mode
	if p.PreferDrafts {
		p.Selection = "reviewed_draft_then_birthday"
	}
	if p.NextCursor < 0 {
		return p, errors.New("неверный курсор шаблонов")
	}
	return p, nil
}

func (s *Store) ReactionMessagePolicy(ctx context.Context, owner int64) (ReactionMessagePolicy, error) {
	if owner <= 0 {
		return ReactionMessagePolicy{}, errors.New("неверный владелец")
	}
	if err := s.initReactionMessages(ctx); err != nil {
		return ReactionMessagePolicy{}, err
	}
	return scanReactionMessagePolicy(s.DB.QueryRowContext(ctx, "SELECT mode,templates_json,telegram_username,prefer_drafts,next_cursor,revision FROM reaction_message_settings WHERE owner_id=?", owner), owner)
}

// Saving templates never starts a worker or sends a reaction.
func (s *Store) SetReactionMessagePolicy(ctx context.Context, owner int64, mode string, templates []string, usernames ...string) (ReactionMessagePolicy, error) {
	if owner <= 0 {
		return ReactionMessagePolicy{}, errors.New("неверный владелец")
	}
	if len(usernames) > 1 {
		return ReactionMessagePolicy{}, errors.New("укажи один Telegram username")
	}
	telegramUsername := ""
	if len(usernames) == 1 {
		telegramUsername = usernames[0]
	}
	return s.ConfigureReactionMessagePolicy(ctx, owner, ReactionMessagePolicyConfig{Mode: mode, Templates: templates, TelegramUsername: telegramUsername})
}

type ReactionMessagePolicyConfig struct {
	Mode             string   `json:"mode"`
	Templates        []string `json:"templates"`
	TelegramUsername string   `json:"telegram_username"`
	PreferDrafts     bool     `json:"prefer_drafts"`
}

func (s *Store) ConfigureReactionMessagePolicy(ctx context.Context, owner int64, config ReactionMessagePolicyConfig) (ReactionMessagePolicy, error) {
	if owner <= 0 {
		return ReactionMessagePolicy{}, errors.New("неверный владелец")
	}
	mode, templates, telegramUsername := config.Mode, config.Templates, config.TelegramUsername
	if config.PreferDrafts && mode != "birthday" {
		return ReactionMessagePolicy{}, errors.New("reviewed drafts require birthday fallback mode")
	}
	if err := validateReactionMessagePolicy(mode, templates, telegramUsername); err != nil {
		return ReactionMessagePolicy{}, err
	}
	if templates == nil {
		templates = []string{}
	}
	raw, err := json.Marshal(templates)
	if err != nil {
		return ReactionMessagePolicy{}, err
	}
	if err = s.initReactionMessages(ctx); err != nil {
		return ReactionMessagePolicy{}, err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return ReactionMessagePolicy{}, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO reaction_message_settings(owner_id,mode,templates_json,telegram_username,prefer_drafts) VALUES(?,?,?,?,?)
 ON CONFLICT(owner_id) DO UPDATE SET mode=excluded.mode,templates_json=excluded.templates_json,telegram_username=excluded.telegram_username,prefer_drafts=excluded.prefer_drafts,next_cursor=0,revision=reaction_message_settings.revision+1,updated_at=CURRENT_TIMESTAMP`, owner, mode, string(raw), telegramUsername, config.PreferDrafts)
	if err != nil {
		return ReactionMessagePolicy{}, err
	}
	p, err := scanReactionMessagePolicy(tx.QueryRowContext(ctx, "SELECT mode,templates_json,telegram_username,prefer_drafts,next_cursor,revision FROM reaction_message_settings WHERE owner_id=?", owner), owner)
	if err != nil {
		return p, err
	}
	return p, tx.Commit()
}

// Commit the exact outgoing intent and rotation together before the sole POST.
func (s *Store) prepareLikeMessage(ctx context.Context, owner, action int64, target StoredProfile, expected Session, explicit *string) ([]byte, error) {
	if err := s.initReactionMessages(ctx); err != nil {
		return nil, err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='reaction_message_settings'"); err != nil {
		return nil, err
	}
	if !s.cfg.AllowReactions {
		return nil, ErrReactionDisabled
	}
	var paused bool
	if err = tx.QueryRowContext(ctx, "SELECT paused FROM users WHERE id=?", owner).Scan(&paused); err != nil {
		return nil, err
	}
	if paused {
		return nil, ErrReactionPaused
	}
	session, err := s.businessSession(ctx, tx, owner)
	if err != nil || session.State != "connected" || session.UserID != expected.UserID || session.Generation != expected.Generation {
		return nil, ErrReactionSessionChanged
	}
	var state string
	if err = tx.QueryRowContext(ctx, "SELECT state FROM actions WHERE id=? AND owner_id=? AND profile_id=? AND kind='Like'", action, owner, target.ID).Scan(&state); err != nil {
		return nil, err
	}
	if state != "pending" {
		return nil, ErrActionBlocked
	}
	var exists bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM reaction_message_intents WHERE action_id=?)", action).Scan(&exists); err != nil {
		return nil, err
	}
	if exists {
		return nil, ErrActionBlocked
	}
	var targetRaw string
	if err = tx.QueryRowContext(ctx, `SELECT v.raw_json FROM queue q JOIN versions v ON v.id=q.version_id AND v.owner_id=q.owner_id AND v.profile_id=q.profile_id WHERE q.owner_id=? AND q.profile_id=?`, owner, target.ID).Scan(&targetRaw); err != nil {
		return nil, err
	}
	currentTarget, err := decodeProfile([]byte(targetRaw))
	if err != nil || currentTarget.ID() != target.ID {
		return nil, errors.New("анкета изменилась; действие отменено")
	}
	if err = reactionDraftHoldTx(ctx, tx, owner, session.UserID, target.ID); err != nil {
		return nil, err
	}
	mode, message, slot, revision := "explicit", "", int64(-1), int64(0)
	reservedCursor := int64(-1)
	if explicit != nil {
		message = *explicit
	} else {
		p, e := scanReactionMessagePolicy(tx.QueryRowContext(ctx, "SELECT mode,templates_json,telegram_username,prefer_drafts,next_cursor,revision FROM reaction_message_settings WHERE owner_id=?", owner), owner)
		if e != nil {
			return nil, e
		}
		mode, revision = p.Mode, p.Revision
		if mode == "birthday" {
			age, known := autoInteger(currentTarget["age"])
			if !known || age < 18 {
				return nil, ErrReactionMessageTargetAge
			}
			if birthdayGreetingMinorDOB(currentTarget, time.Now().UTC()) {
				return nil, ErrReactionMessageTargetAge
			}
			found := false
			if p.PreferDrafts {
				message, found, err = reactionDraftMessageTx(ctx, tx, owner, session.UserID, target.ID, p.TelegramUsername, currentTarget)
				if err != nil {
					return nil, err
				}
			}
			if found {
				mode = "personal"
			} else {
				message, slot, err = birthdayReactionMessage(currentTarget, p.TelegramUsername, p.NextCursor)
				if err != nil {
					return nil, err
				}
			}
		} else if mode != "off" {
			slot = 0
			if mode == "rotate" {
				slot = p.NextCursor % int64(len(p.Templates))
			}
			message = p.Templates[slot]
		}
		if mode == "rotate" || mode == "birthday" {
			reservedCursor = p.NextCursor
			if _, err = tx.ExecContext(ctx, "UPDATE reaction_message_settings SET next_cursor=next_cursor+1,updated_at=CURRENT_TIMESTAMP WHERE owner_id=? AND revision=?", owner, revision); err != nil {
				return nil, err
			}
		}
	}
	if err = validateReactionMessage(message); err != nil {
		return nil, err
	}
	if message != "" {
		age, known := autoInteger(currentTarget["age"])
		if !known || age < 18 {
			return nil, ErrReactionMessageTargetAge
		}
	}
	request, err := json.Marshal(map[string]any{"reactionType": 1, "isDeleted": false, "userId": target.ID, "fromUserId": session.UserID, "message": message})
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO reaction_message_intents(action_id,owner_id,upstream_id,profile_id,generation,message,request_body,message_mode,policy_revision,template_index,reserved_cursor) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, action, owner, session.UserID, target.ID, session.Generation, message, request, mode, revision, slot, reservedCursor); err != nil {
		return nil, err
	}
	return request, tx.Commit()
}

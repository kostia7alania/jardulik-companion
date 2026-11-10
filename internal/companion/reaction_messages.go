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
 templates_json TEXT NOT NULL DEFAULT '[]', telegram_username TEXT NOT NULL DEFAULT '', prefer_drafts INTEGER NOT NULL DEFAULT 0 CHECK(prefer_drafts IN (0,1)), iron_mix INTEGER NOT NULL DEFAULT 0 CHECK(iron_mix IN (0,1)), super_before_age INTEGER NOT NULL DEFAULT 0 CHECK(super_before_age=0 OR super_before_age BETWEEN 18 AND 112), next_cursor INTEGER NOT NULL DEFAULT 0 CHECK(next_cursor>=0),
 revision INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS reaction_message_intents (
 action_id INTEGER PRIMARY KEY REFERENCES actions(id) ON DELETE CASCADE,
 owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 upstream_id INTEGER NOT NULL, profile_id INTEGER NOT NULL, generation TEXT NOT NULL,
 message TEXT NOT NULL, request_body BLOB NOT NULL, message_mode TEXT NOT NULL,
 policy_revision INTEGER NOT NULL, template_index INTEGER NOT NULL, reserved_cursor INTEGER NOT NULL DEFAULT -1, reaction_type INTEGER NOT NULL DEFAULT 1 CHECK(reaction_type IN (1,3)), attempted_at_ms INTEGER NOT NULL DEFAULT 0,
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
	IronMix          bool     `json:"iron_mix"`
	SuperBeforeAge   int      `json:"super_before_age"`
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
	if _, err = tx.ExecContext(ctx, reactionMessageSchema+reactionMessageDraftTable+reactionMessageHoldResolutionTable); err != nil {
		return err
	}
	for _, column := range []string{"prefer_drafts", "iron_mix"} {
		var present int
		if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM pragma_table_info('reaction_message_settings') WHERE name=?", column).Scan(&present); err != nil {
			return err
		}
		if present == 0 {
			if _, err = tx.ExecContext(ctx, "ALTER TABLE reaction_message_settings ADD COLUMN "+column+" INTEGER NOT NULL DEFAULT 0 CHECK("+column+" IN (0,1))"); err != nil {
				return err
			}
		}
	}
	var present int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM pragma_table_info('reaction_message_settings') WHERE name='super_before_age'").Scan(&present); err != nil {
		return err
	}
	if present == 0 {
		if _, err = tx.ExecContext(ctx, "ALTER TABLE reaction_message_settings ADD COLUMN super_before_age INTEGER NOT NULL DEFAULT 0 CHECK(super_before_age=0 OR super_before_age BETWEEN 18 AND 112)"); err != nil {
			return err
		}
	}
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM pragma_table_info('reaction_message_intents') WHERE name='reaction_type'").Scan(&present); err != nil {
		return err
	}
	if present == 0 {
		if _, err = tx.ExecContext(ctx, "ALTER TABLE reaction_message_intents ADD COLUMN reaction_type INTEGER NOT NULL DEFAULT 1 CHECK(reaction_type IN (1,3))"); err != nil {
			return err
		}
	}
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM pragma_table_info('reaction_message_intents') WHERE name='attempted_at_ms'").Scan(&present); err != nil {
		return err
	}
	if present == 0 {
		if _, err = tx.ExecContext(ctx, "ALTER TABLE reaction_message_intents ADD COLUMN attempted_at_ms INTEGER NOT NULL DEFAULT 0"); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func scanReactionMessagePolicy(row interface{ Scan(...any) error }, owner int64) (ReactionMessagePolicy, error) {
	p := ReactionMessagePolicy{OwnerID: owner, Mode: "off", Templates: []string{}, Selection: "off"}
	var raw string
	err := row.Scan(&p.Mode, &raw, &p.TelegramUsername, &p.PreferDrafts, &p.IronMix, &p.SuperBeforeAge, &p.NextCursor, &p.Revision)
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
	if !validSuperBeforeAge(p.SuperBeforeAge) {
		return p, errors.New("неверное правило суперлайков")
	}
	if p.SuperBeforeAge != 0 && p.Mode == "off" {
		return p, errors.New("для суперлайков включи сообщения")
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
	return scanReactionMessagePolicy(s.DB.QueryRowContext(ctx, "SELECT mode,templates_json,telegram_username,prefer_drafts,iron_mix,super_before_age,next_cursor,revision FROM reaction_message_settings WHERE owner_id=?", owner), owner)
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
	IronMix          bool     `json:"iron_mix"`
	SuperBeforeAge   int      `json:"super_before_age"`
}

func (s *Store) ConfigureReactionMessagePolicy(ctx context.Context, owner int64, config ReactionMessagePolicyConfig) (ReactionMessagePolicy, error) {
	if owner <= 0 {
		return ReactionMessagePolicy{}, errors.New("неверный владелец")
	}
	mode, templates, telegramUsername := config.Mode, config.Templates, config.TelegramUsername
	if !validSuperBeforeAge(config.SuperBeforeAge) {
		return ReactionMessagePolicy{}, errors.New("правило суперлайков: 0 или возраст от 18 до 112")
	}
	if config.SuperBeforeAge != 0 && mode == "off" {
		return ReactionMessagePolicy{}, errors.New("для суперлайков включи сообщения")
	}
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
	_, err = tx.ExecContext(ctx, `INSERT INTO reaction_message_settings(owner_id,mode,templates_json,telegram_username,prefer_drafts,iron_mix,super_before_age) VALUES(?,?,?,?,?,?,?)
 ON CONFLICT(owner_id) DO UPDATE SET mode=excluded.mode,templates_json=excluded.templates_json,telegram_username=excluded.telegram_username,prefer_drafts=excluded.prefer_drafts,iron_mix=excluded.iron_mix,super_before_age=excluded.super_before_age,next_cursor=0,revision=reaction_message_settings.revision+1,updated_at=CURRENT_TIMESTAMP`, owner, mode, string(raw), telegramUsername, config.PreferDrafts, config.IronMix, config.SuperBeforeAge)
	if err != nil {
		return ReactionMessagePolicy{}, err
	}
	p, err := scanReactionMessagePolicy(tx.QueryRowContext(ctx, "SELECT mode,templates_json,telegram_username,prefer_drafts,iron_mix,super_before_age,next_cursor,revision FROM reaction_message_settings WHERE owner_id=?", owner), owner)
	if err != nil {
		return p, err
	}
	return p, tx.Commit()
}

// Commit the exact outgoing intent and rotation together before the sole POST.
func (s *Store) prepareLikeMessage(ctx context.Context, owner, action int64, target StoredProfile, expected Session, explicit *string) ([]byte, error) {
	return s.preparePositiveMessage(ctx, owner, action, target, expected, explicit, PositiveLike, false)
}

func (s *Store) preparePositiveMessage(ctx context.Context, owner, action int64, target StoredProfile, expected Session, explicit *string, kind PositiveReactionType, automatic bool) ([]byte, error) {
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
	age, knownAge := autoInteger(currentTarget["age"])
	if !knownAge || age < 18 || age > 111 || birthdayGreetingMinorDOB(currentTarget, time.Now().UTC()) {
		return nil, ErrReactionMessageTargetAge
	}
	policy, err := scanReactionMessagePolicy(tx.QueryRowContext(ctx, "SELECT mode,templates_json,telegram_username,prefer_drafts,iron_mix,super_before_age,next_cursor,revision FROM reaction_message_settings WHERE owner_id=?", owner), owner)
	if err != nil {
		return nil, err
	}
	if automatic {
		kind = positiveReactionForAge(age, policy.SuperBeforeAge)
		filters, e := s.currentPositiveFiltersTx(ctx, tx, owner, session.UserID)
		if e != nil {
			return nil, e
		}
		if age < filters.AgeMin || age > filters.AgeMax {
			return nil, errors.New("анкета вне текущего возраста поиска")
		}
	}
	if kind != PositiveLike && kind != PositiveSuperLike {
		return nil, errors.New("invalid positive reaction")
	}
	if automatic && kind == PositiveLike {
		var fireWait bool
		if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM actions WHERE owner_id=? AND profile_id=? AND kind='Like' AND state='fire_waiting')", owner, target.ID).Scan(&fireWait); err != nil {
			return nil, err
		}
		if fireWait {
			return nil, ErrReactionFireUnavailable
		}
	}
	mode, message, slot, revision := "explicit", "", int64(-1), int64(0)
	reservedCursor := int64(-1)
	if explicit != nil {
		message = *explicit
	} else {
		p := policy
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
		if p.IronMix {
			message = ironMixReactionMessage(message, birthdayGreetingName(currentTarget))
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
	if kind == PositiveSuperLike {
		if strings.TrimSpace(message) == "" {
			return nil, ErrInvalidReactionMessage
		}
		var ownRaw string
		if err = tx.QueryRowContext(ctx, "SELECT account_json FROM sessions WHERE owner_id=?", owner).Scan(&ownRaw); err != nil {
			return nil, err
		}
		own, e := decodeProfile([]byte(ownRaw))
		if e != nil || own.ID() != session.UserID {
			return nil, ErrReactionSessionChanged
		}
		fire, known := autoInteger(own["fire"])
		if !known || fire < 0 {
			return nil, ErrReactionFireUnknown
		}
		if fire == 0 {
			return nil, ErrReactionFireUnavailable
		}
	}
	request, err := json.Marshal(map[string]any{"reactionType": kind, "isDeleted": false, "userId": target.ID, "fromUserId": session.UserID, "message": message})
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO reaction_message_intents(action_id,owner_id,upstream_id,profile_id,generation,message,request_body,message_mode,policy_revision,template_index,reserved_cursor,reaction_type) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, action, owner, session.UserID, target.ID, session.Generation, message, request, mode, revision, slot, reservedCursor, kind); err != nil {
		return nil, err
	}
	if audit := claimedPositiveAuditID(ctx); audit > 0 {
		r, e := tx.ExecContext(ctx, `UPDATE debug_api_calls SET action_id=? WHERE id=? AND owner_id=? AND upstream_id=? AND target_id=? AND reaction_type=? AND state='sending' AND action_id IS NULL AND request_body=?`, action, audit, owner, session.UserID, target.ID, kind, request)
		if e != nil {
			return nil, e
		}
		n, e := r.RowsAffected()
		if e != nil || n != 1 {
			return nil, errors.New("debug positive intent binding changed")
		}
	}
	return request, tx.Commit()
}

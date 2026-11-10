package companion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

func openingTableTx(ctx context.Context, tx *sql.Tx, table string) (bool, error) {
	var found bool
	err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?)", table).Scan(&found)
	return found, err
}

func openingPostKeysTx(ctx context.Context, tx *sql.Tx, owner, upstream int64) ([]string, error) {
	keys := []string{"formula"}
	for _, source := range []struct{ table, prefix, query string }{
		{"reaction_message_drafts", "draft:", "SELECT profile_id FROM reaction_message_drafts WHERE owner_id=? AND upstream_id=? AND state='approved' GROUP BY profile_id ORDER BY profile_id"},
		{"reaction_message_intents", "intent:", "SELECT i.action_id FROM reaction_message_intents i JOIN actions a ON a.id=i.action_id AND a.owner_id=i.owner_id AND a.profile_id=i.profile_id WHERE i.owner_id=? AND i.upstream_id=? AND i.message<>'' AND a.state IN ('succeeded','unknown') ORDER BY i.action_id"},
		{"debug_api_calls", "debug:", "SELECT id FROM debug_api_calls WHERE owner_id=? AND upstream_id=? AND path='/reactions/addNew' AND reaction_type=3 AND action_id IS NULL AND state='received' AND http_status=200 ORDER BY id"},
	} {
		present, err := openingTableTx(ctx, tx, source.table)
		if err != nil {
			return nil, err
		}
		if !present {
			continue
		}
		rows, err := tx.QueryContext(ctx, source.query, owner, upstream)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id int64
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			keys = append(keys, source.prefix+strconv.FormatInt(id, 10))
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func openingPolicyTx(ctx context.Context, tx *sql.Tx, owner int64) (ReactionMessagePolicy, error) {
	present, err := openingTableTx(ctx, tx, "reaction_message_settings")
	if err != nil {
		return ReactionMessagePolicy{}, err
	}
	if !present {
		return ReactionMessagePolicy{OwnerID: owner, Mode: "off"}, nil
	}
	return scanReactionMessagePolicy(tx.QueryRowContext(ctx, "SELECT mode,templates_json,telegram_username,prefer_drafts,iron_mix,super_before_age,next_cursor,revision FROM reaction_message_settings WHERE owner_id=?", owner), owner)
}

func openingProfileTx(ctx context.Context, tx *sql.Tx, owner, id int64) (Profile, bool, error) {
	var raw string
	err := tx.QueryRowContext(ctx, `SELECT v.raw_json FROM queue q JOIN versions v ON v.id=q.version_id AND v.owner_id=q.owner_id AND v.profile_id=q.profile_id WHERE q.owner_id=? AND q.profile_id=?`, owner, id).Scan(&raw)
	current := err == nil
	if errors.Is(err, sql.ErrNoRows) {
		err = tx.QueryRowContext(ctx, "SELECT raw_json FROM versions WHERE owner_id=? AND profile_id=? ORDER BY id DESC LIMIT 1", owner, id).Scan(&raw)
	}
	if err != nil {
		return nil, false, err
	}
	p, err := decodeProfile([]byte(raw))
	if err != nil || p.ID() != id {
		return nil, false, errOpeningPostSource
	}
	return p, current, nil
}

func (s *Service) openingPostItemTx(ctx context.Context, tx *sql.Tx, expected Session, key string) (openingPostItem, error) {
	if key == "formula" {
		policy, err := openingPolicyTx(ctx, tx, s.Config.OwnerID)
		if err != nil {
			return openingPostItem{}, err
		}
		policy.NextCursor = 0 // The formula describes rotation but does not preview a particular slot.
		raw, err := json.Marshal(policy)
		return openingPostItem{Key: key, Kind: "formula", Payload: openingFormulaPayload(policy), Source: raw}, err
	}
	kind, number, found := strings.Cut(key, ":")
	id, err := strconv.ParseInt(number, 10, 64)
	if !found || err != nil || id <= 0 {
		return openingPostItem{}, sql.ErrNoRows
	}
	item := openingPostItem{Key: key, Kind: kind}
	var quote, status, rule string
	var evidence []reactionDraftEvidence
	var profile Profile
	if kind == "draft" {
		item.Profile = id
		var current bool
		profile, current, err = openingProfileTx(ctx, tx, s.Config.OwnerID, id)
		if err != nil {
			return item, err
		}
		policy, e := openingPolicyTx(ctx, tx, s.Config.OwnerID)
		if e != nil {
			return item, e
		}
		currentBasis, currentHash, e := reactionDraftContent(profile)
		if e != nil {
			return item, errOpeningPostSource
		}
		var draftID int64
		var handle, hash, binding, message, captureSHA string
		err = tx.QueryRowContext(ctx, `SELECT id,telegram_username,content_hash,basis_hash,message,capture_json,capture_sha FROM reaction_message_drafts
 WHERE owner_id=? AND upstream_id=? AND profile_id=? AND state='approved'
 ORDER BY CASE WHEN telegram_username=? AND content_hash=? THEN 0 WHEN telegram_username=? THEN 1 ELSE 2 END,id DESC LIMIT 1`, s.Config.OwnerID, expected.UserID, id, policy.TelegramUsername, currentHash, policy.TelegramUsername).Scan(&draftID, &handle, &hash, &binding, &message, &item.Source, &captureSHA)
		if err != nil {
			return item, err
		}
		input, _, parsedHash, e := parseReactionDraft(item.Source)
		if e != nil || input.State != "approved" || input.ProfileID != id || input.Message != message || !reactionDraftHasHandle(message, handle) || parsedHash != hash || digest(item.Source) != captureSHA || binding != reactionDraftBindingHash(s.Config.OwnerID, expected.UserID, id, handle, hash) {
			return item, sql.ErrNoRows
		}
		status = "Предпросмотр · отправка не выполнялась"
		personal := false
		if !current {
			rule = "Нет текущей анкеты в очереди; текст сейчас не выбран."
		} else {
			quote, rule, personal, err = openingPreviewTx(ctx, tx, s.Config.OwnerID, expected.UserID, id, policy, profile)
			if err != nil {
				return item, err
			}
			if personal {
				var review reactionDraftReview
				if json.Unmarshal(input.Review, &review) != nil {
					return item, sql.ErrNoRows
				}
				evidence = review.Evidence
			}
		}
		if personal {
			rule += fmt.Sprintf("\nИсточник: approved draft #%d.", draftID)
		} else {
			rule += fmt.Sprintf("\nАрхив заготовки: #%d; текущий выбор указан выше.", draftID)
		}
		if personal || policy.Mode != "rotate" && policy.Mode != "birthday" {
			policy.NextCursor = 0
		}
		item.Source, err = json.Marshal(map[string]any{"draft_capture_bytes": item.Source, "current_basis": currentBasis, "birthday": profile.String("birthday"), "policy": policy, "current_queue": current})
		if err != nil {
			return item, err
		}
	} else if kind == "intent" {
		var state, mode, response string
		var revision, template, reaction int64
		err = tx.QueryRowContext(ctx, `SELECT i.profile_id,i.message,i.request_body,i.message_mode,i.policy_revision,i.template_index,i.reaction_type,a.state,a.response
 FROM reaction_message_intents i JOIN actions a ON a.id=i.action_id AND a.owner_id=i.owner_id AND a.profile_id=i.profile_id
 WHERE i.action_id=? AND i.owner_id=? AND i.upstream_id=? AND a.state IN ('succeeded','unknown')`, id, s.Config.OwnerID, expected.UserID).Scan(&item.Profile, &quote, &item.Source, &mode, &revision, &template, &reaction, &state, &response)
		if err != nil {
			return item, err
		}
		if !openingFrozenRequest(item.Source, expected.UserID, item.Profile, reaction, quote) {
			return item, sql.ErrNoRows
		}
		status = "Результат отправки неизвестен"
		if state == "succeeded" {
			result, e := decodeReactionResult([]byte(response), time.Now().UTC())
			if e != nil || result.Decision != ReactionSuccess {
				return item, sql.ErrNoRows
			}
			status = "Отправка подтверждена API Jardulik"
		}
		switch mode {
		case "personal", "birthday", "explicit", "first", "rotate", "off":
		default:
			mode = "unknown"
		}
		rule = fmt.Sprintf("Факт: intent #%d · type=%d · mode=%s · revision=%d · template=%d.\nТекст взят из сохранённого запроса, без повторного рендера. Это не подтверждение прочтения.", id, reaction, mode, revision, template)
		profile, _, err = openingProfileTx(ctx, tx, s.Config.OwnerID, item.Profile)
		if err != nil {
			return item, err
		}
	} else if kind == "debug" {
		var response []byte
		err = tx.QueryRowContext(ctx, `SELECT target_id,request_body,response_body FROM debug_api_calls WHERE id=? AND owner_id=? AND upstream_id=? AND path='/reactions/addNew' AND reaction_type=3 AND action_id IS NULL AND state='received' AND http_status=200`, id, s.Config.OwnerID, expected.UserID).Scan(&item.Profile, &item.Source, &response)
		if err != nil {
			return item, err
		}
		var request struct {
			Message string `json:"message"`
		}
		result, e := decodeReactionResult(response, time.Now().UTC())
		if json.Unmarshal(item.Source, &request) != nil || !openingFrozenRequest(item.Source, expected.UserID, item.Profile, 3, request.Message) || e != nil || result.Decision != ReactionSuccess {
			return item, sql.ErrNoRows
		}
		quote, status = request.Message, "Отправка подтверждена API Jardulik"
		rule = fmt.Sprintf("Факт: прежний debug SuperLike, type=3 · receipt #%d.\nТекст взят из сохранённого запроса, без повторного рендера. Это не подтверждение прочтения.", id)
		profile, _, err = openingProfileTx(ctx, tx, s.Config.OwnerID, item.Profile)
		if err != nil {
			return item, err
		}
	} else {
		return item, sql.ErrNoRows
	}
	var thread int64
	if err = tx.QueryRowContext(ctx, "SELECT thread_id FROM mirror_topics WHERE profile_id=? AND thread_id>0 AND thread_id<>?", item.Profile, s.Config.BackupThreadID).Scan(&thread); err != nil {
		return item, err
	}
	link := forumTopicURL(s.Config.BackupChatID, thread)
	if link == "" {
		return item, sql.ErrNoRows
	}
	item.Payload = openingRecipientPayload(profile, item.Profile, link, status, quote, rule, evidence)
	return item, nil
}

func openingPreviewTx(ctx context.Context, tx *sql.Tx, owner, upstream, profileID int64, policy ReactionMessagePolicy, profile Profile) (message, rule string, personal bool, err error) {
	age, known := autoInteger(profile["age"])
	if !known || age < 18 || age > 111 || birthdayGreetingMinorDOB(profile, time.Now().UTC()) {
		return "", "Отправка остановлена: нет подтверждённого возраста 18+.", false, nil
	}
	if e := reactionDraftHoldTx(ctx, tx, owner, upstream, profileID); e != nil {
		if errors.Is(e, ErrReactionMessageReviewHold) {
			return "", "Отправка остановлена: активный hold.", false, nil
		}
		return "", "", false, e
	}
	switch policy.Mode {
	case "off":
		return "", "Сообщения сейчас выключены.", false, nil
	case "birthday":
		if policy.PreferDrafts {
			message, personal, err = reactionDraftMessageTx(ctx, tx, owner, upstream, profileID, policy.TelegramUsername, profile)
			if err != nil {
				return
			}
		}
		if personal {
			rule = "Персональная заготовка: owner/account/handle и strict8 совпадают."
		} else {
			var index int64
			message, index, err = birthdayReactionMessage(profile, policy.TelegramUsername, policy.NextCursor)
			if err != nil {
				return
			}
			rule = fmt.Sprintf("Запасной birthday: календарная тема %s · template=%d · cursor не изменён.", birthdayGreetingSign(profile, time.Now().UTC()), index)
		}
	case "first", "rotate":
		index := int64(0)
		if policy.Mode == "rotate" {
			index = policy.NextCursor % int64(len(policy.Templates))
		}
		message, rule = policy.Templates[index], fmt.Sprintf("Свой шаблон #%d · mode=%s · cursor не изменён.", index+1, policy.Mode)
	default:
		return "", "", false, errors.New("invalid opening preview policy")
	}
	if policy.IronMix {
		message = ironMixReactionMessage(message, birthdayGreetingName(profile))
		rule += "\nIron: изменяется только известное приветствие."
	}
	err = validateReactionMessage(message)
	return
}

func openingFrozenRequest(raw []byte, upstream, target, reaction int64, message string) bool {
	if !utf8.Valid(raw) || strings.TrimSpace(message) == "" || validateReactionMessage(message) != nil {
		return false
	}
	fields, err := debugObject(raw, "fromUserId", "userId", "reactionType", "isDeleted", "message")
	if err != nil || len(fields) != 5 || !bytes.Equal(bytes.TrimSpace(fields["isDeleted"]), []byte("false")) {
		return false
	}
	var request struct {
		From    int64  `json:"fromUserId"`
		To      int64  `json:"userId"`
		Type    int64  `json:"reactionType"`
		Deleted bool   `json:"isDeleted"`
		Message string `json:"message"`
	}
	return json.Unmarshal(raw, &request) == nil && request.From == upstream && request.To == target && request.Type == reaction && (reaction == 1 || reaction == 3) && !request.Deleted && request.Message == message
}

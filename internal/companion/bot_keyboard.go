package companion

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

const (
	keyboardLike = "❤️ Лайк"
	keyboardNext = "➡️ Следующая"
	keyboardSave = "⭐ Сохранить"
	keyboardMenu = "📋 Меню"
)

func keyboardReplyMarkup() any {
	return map[string]any{
		"keyboard": [][]map[string]string{
			{{"text": keyboardLike}, {"text": keyboardNext}},
			{{"text": keyboardSave}, {"text": keyboardMenu}},
		},
		"resize_keyboard": true, "is_persistent": true, "one_time_keyboard": false,
		"input_field_placeholder": "Выбери действие",
	}
}

type keyboardSessionPin struct {
	Generation string
	UpstreamID int64
}

func (s *Service) ensureKeyboardState(ctx context.Context) error {
	_, err := s.Store.DB.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS bot_keyboard_state (
	 owner_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
	 profile_id INTEGER NOT NULL DEFAULT 0, version_id INTEGER NOT NULL DEFAULT 0,
	 generation TEXT NOT NULL DEFAULT '', upstream_id INTEGER NOT NULL DEFAULT 0,
	 last_message_id INTEGER NOT NULL DEFAULT 0, installation_state TEXT NOT NULL DEFAULT ''
	)`)
	return err
}

func (s *Service) keyboardSessionPin(ctx context.Context, owner int64) (keyboardSessionPin, error) {
	session, err := s.Store.Session(ctx, owner)
	if err != nil {
		return keyboardSessionPin{}, err
	}
	if session.State != "connected" || session.UserID <= 0 || session.Generation == "" {
		return keyboardSessionPin{}, errors.New("keyboard session unavailable")
	}
	return keyboardSessionPin{session.Generation, session.UserID}, nil
}

// Clear before attempting a new delivery. An unknown send must not leave the old
// profile as the target of a generic keyboard action.
func (s *Service) clearActiveCard(ctx context.Context, owner int64) error {
	if err := s.ensureKeyboardState(ctx); err != nil {
		return err
	}
	_, err := s.Store.DB.ExecContext(ctx, `UPDATE bot_keyboard_state SET profile_id=0,version_id=0,generation='',upstream_id=0,last_message_id=0 WHERE owner_id=?`, owner)
	return err
}

// The caller holds userLock. Only confirmed private-chat message IDs belong here.
func (s *Service) setActiveCard(ctx context.Context, owner int64, p StoredProfile, pin keyboardSessionPin, ids []int64) error {
	if err := s.ensureKeyboardState(ctx); err != nil {
		return err
	}
	var last int64
	for _, id := range ids {
		if id <= 0 {
			return errors.New("unconfirmed keyboard card")
		}
		if id > last {
			last = id
		}
	}
	if last == 0 {
		return errors.New("unconfirmed keyboard card")
	}
	version, err := s.Store.Version(ctx, owner, p.VersionID)
	if err != nil || version.ID != p.ID {
		return errors.New("keyboard card owner mismatch")
	}
	mu := s.Store.SessionMutex(owner)
	mu.Lock()
	defer mu.Unlock()
	current, err := s.keyboardSessionPin(ctx, owner)
	if err != nil {
		return err
	}
	if current != pin {
		return ErrReactionSessionChanged
	}
	_, err = s.Store.DB.ExecContext(ctx, `INSERT INTO bot_keyboard_state(owner_id,profile_id,version_id,generation,upstream_id,last_message_id) VALUES(?,?,?,?,?,?)
	 ON CONFLICT(owner_id) DO UPDATE SET profile_id=excluded.profile_id,version_id=excluded.version_id,generation=excluded.generation,upstream_id=excluded.upstream_id,last_message_id=excluded.last_message_id`, owner, p.ID, p.VersionID, pin.Generation, pin.UpstreamID, last)
	return err
}

func (s *Service) activeKeyboardCard(ctx context.Context, owner int64, messageID int64) (StoredProfile, error) {
	if err := s.ensureKeyboardState(ctx); err != nil {
		return StoredProfile{}, err
	}
	var profileID, versionID, last int64
	var pin keyboardSessionPin
	err := s.Store.DB.QueryRowContext(ctx, `SELECT profile_id,version_id,generation,upstream_id,last_message_id FROM bot_keyboard_state WHERE owner_id=?`, owner).Scan(&profileID, &versionID, &pin.Generation, &pin.UpstreamID, &last)
	if err != nil {
		return StoredProfile{}, err
	}
	if profileID <= 0 || (messageID > 0 && messageID <= last) {
		return StoredProfile{}, errors.New("keyboard card unavailable or changed")
	}
	current, err := s.keyboardSessionPin(ctx, owner)
	if err != nil {
		return StoredProfile{}, err
	}
	if current != pin {
		return StoredProfile{}, ErrReactionSessionChanged
	}
	p, err := s.Store.Version(ctx, owner, versionID)
	if err == nil && p.ID != profileID {
		err = errors.New("keyboard card owner mismatch")
	}
	return p, err
}

// Called only under the verified private owner's userLock, before filter input.
func (s *Service) handleReplyKeyboardLocked(ctx context.Context, owner int64, m Message) bool {
	if m.Chat.Type != "private" || m.Chat.ID != owner || m.From.ID != owner {
		return false
	}
	switch m.Text {
	case keyboardMenu:
		s.showKeyboardMenuLocked(ctx, owner)
		return true
	case keyboardNext:
		if err := s.clearActiveCard(ctx, owner); err != nil {
			_ = s.say(ctx, owner, "Не удалось открыть следующую анкету. Попробуй ещё раз.", nil)
			return true
		}
		s.next(ctx, owner)
		return true
	case keyboardLike, keyboardSave:
		p, err := s.activeKeyboardCard(ctx, owner, m.ID)
		if err != nil || m.ID <= 0 {
			_ = s.say(ctx, owner, "Нажми ➡️ Следующая, чтобы открыть анкету.", nil)
			return true
		}
		if m.Text == keyboardSave {
			if err := s.Store.Favorite(ctx, owner, p.ID); err != nil {
				_ = s.say(ctx, owner, "Не удалось сохранить анкету.", nil)
			} else {
				_ = s.say(ctx, owner, "⭐ Сохранено в избранное.", nil)
			}
		} else if s.manualLike(ctx, owner, p.ID, func(guardCtx context.Context) error {
			current, err := s.activeKeyboardCard(guardCtx, owner, m.ID)
			if err != nil || current.ID != p.ID || current.VersionID != p.VersionID {
				return ErrReactionSessionChanged
			}
			return nil
		}) {
			if err := s.clearActiveCard(ctx, owner); err == nil {
				s.next(ctx, owner)
			}
		}
		return true
	}
	return false
}

func (s *Service) showKeyboardMenuLocked(ctx context.Context, owner int64) {
	b := func(text, data string) map[string]string {
		return map[string]string{"text": text, "callback_data": data}
	}
	rows := [][]map[string]string{}
	if p, err := s.activeKeyboardCard(ctx, owner, 0); err == nil {
		rows = append(rows, []map[string]string{b("👤 Текущая анкета", fmt.Sprintf("p:%d", p.ID))})
		rows = append(rows, []map[string]string{b("📄 Полный текст", fmt.Sprintf("t:%d", p.ID)), b("JSON", fmt.Sprintf("jv:%d", p.VersionID))})
		if button := s.telegramContactButton(ctx, owner, p.ID); button != nil {
			rows = append(rows, []map[string]string{button})
		}
	}
	rows = append(rows,
		[]map[string]string{b("⭐ Избранное", "favorites"), b("❤️ Лайки", "likes"), b("💞 Взаимные", "matches")},
		[]map[string]string{b("⚙️ Настройки", "settings"), b("🔎 Фильтры", "filters")},
		[]map[string]string{b("➡️ Следующая", "next"), jardulikAppButton()},
	)
	if owner == s.Config.OwnerID {
		rows = append(rows, []map[string]string{b("📅 По возрасту", "ai:0")})
	}
	_ = s.say(ctx, owner, "📋 Меню", map[string]any{"inline_keyboard": rows})
}

// Claim before sending: an interrupted installation is never blindly replayed.
// Called under userLock; explicit /start may independently restore the keyboard.
func (s *Service) installKeyboardOnceLocked(ctx context.Context, owner int64) error {
	if _, err := s.keyboardSessionPin(ctx, owner); err != nil {
		return err
	}
	if err := s.ensureKeyboardState(ctx); err != nil {
		return err
	}
	if _, err := s.Store.DB.ExecContext(ctx, `INSERT OR IGNORE INTO bot_keyboard_state(owner_id) VALUES(?)`, owner); err != nil {
		return err
	}
	result, err := s.Store.DB.ExecContext(ctx, `UPDATE bot_keyboard_state SET installation_state='sending' WHERE owner_id=? AND installation_state=''`, owner)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil || n == 0 {
		return err
	}
	m, sendErr := s.TG.SendText(ctx, owner, 0, "Кнопки внизу: лайк, следующая анкета, сохранить и меню.", keyboardReplyMarkup())
	state := "unknown"
	if sendErr == nil && m.ID > 0 && m.Chat.ID == owner {
		state = "sent"
	} else if sendErr == nil {
		sendErr = errors.New("keyboard delivery unconfirmed")
	}
	_, saveErr := s.Store.DB.ExecContext(context.WithoutCancel(ctx), `UPDATE bot_keyboard_state SET installation_state=? WHERE owner_id=? AND installation_state='sending'`, state, owner)
	return errors.Join(sendErr, saveErr)
}

func (s *Service) bootstrapKeyboards(ctx context.Context) error {
	if err := s.ensureKeyboardState(ctx); err != nil {
		return err
	}
	rows, err := s.Store.DB.QueryContext(ctx, `SELECT s.owner_id FROM sessions s LEFT JOIN bot_keyboard_state k ON k.owner_id=s.owner_id WHERE s.state='connected' AND COALESCE(k.installation_state,'')='' ORDER BY s.owner_id`)
	if err != nil {
		return err
	}
	var owners []int64
	for rows.Next() {
		var owner int64
		if err = rows.Scan(&owner); err != nil {
			break
		}
		owners = append(owners, owner)
	}
	err = errors.Join(err, rows.Err(), rows.Close())
	if err != nil {
		return err
	}
	var failures []error
	for _, owner := range owners {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(failures, err)...)
		}
		mu := s.userLock(owner)
		mu.Lock()
		err := s.installKeyboardOnceLocked(ctx, owner)
		mu.Unlock()
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

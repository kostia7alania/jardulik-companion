package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const businessContactsSchema = `
CREATE TABLE IF NOT EXISTS business_contact_connections (
 id TEXT PRIMARY KEY, owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 upstream_id INTEGER NOT NULL, session_generation TEXT NOT NULL, user_chat_id INTEGER NOT NULL,
 connection_date INTEGER NOT NULL, enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
 update_id INTEGER NOT NULL, raw_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS business_contact_owner ON business_contact_connections(owner_id,update_id);
CREATE TABLE IF NOT EXISTS profile_business_contacts (
 id INTEGER PRIMARY KEY AUTOINCREMENT, owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 upstream_id INTEGER NOT NULL, profile_id INTEGER NOT NULL, source_bot_id INTEGER NOT NULL,
 connection_id TEXT NOT NULL, source_message_id INTEGER NOT NULL, source_message_timestamp INTEGER NOT NULL,
 update_id INTEGER NOT NULL UNIQUE, observed_at_ms INTEGER NOT NULL,
 contact_url TEXT NOT NULL, profile_url TEXT NOT NULL, raw_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS business_contact_latest ON profile_business_contacts(owner_id,upstream_id,profile_id,source_message_timestamp DESC,update_id DESC);
CREATE TABLE IF NOT EXISTS business_contact_inbox (
 id INTEGER PRIMARY KEY AUTOINCREMENT, owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 upstream_id INTEGER NOT NULL, source_bot_id INTEGER NOT NULL, connection_id TEXT NOT NULL,
 source_message_id INTEGER NOT NULL, source_message_timestamp INTEGER NOT NULL,
 update_id INTEGER NOT NULL UNIQUE, captured_at_ms INTEGER NOT NULL,
 parse_state TEXT NOT NULL CHECK(parse_state IN ('unresolved','contact')), raw_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS business_contact_inbox_owner ON business_contact_inbox(owner_id,upstream_id,id);
`

func (s *Store) initBusinessContacts(ctx context.Context) error {
	_, err := s.DB.ExecContext(ctx, businessContactsSchema)
	return err
}

func businessConnectionID(id string) bool {
	return id != "" && len(id) <= 256 && strings.TrimSpace(id) == id && strings.IndexFunc(id, unicode.IsControl) < 0
}

// IngestBusinessContactUpdate commits a receipt before the polling offset can advance.
// Only Telegram's original business update is accepted; ordinary forwards are not evidence.
func (s *Store) IngestBusinessContactUpdate(ctx context.Context, update Update) (bool, error) {
	if update.BusinessConnection == nil && update.BusinessMessage == nil && update.EditedBusinessMessage == nil {
		return false, nil
	}
	if len(update.RawJSON) == 0 || len(update.RawJSON) > 16<<20 {
		return true, errors.New("business contact update has no original JSON")
	}
	var source Update
	if err := json.Unmarshal(update.RawJSON, &source); err != nil || source.ID != update.ID || source.ID <= 0 {
		return true, errors.New("invalid original business contact update")
	}
	count := 0
	for _, present := range []bool{source.BusinessConnection != nil, source.BusinessMessage != nil, source.EditedBusinessMessage != nil} {
		if present {
			count++
		}
	}
	if count != 1 || source.Message != nil || source.Callback != nil {
		return true, errors.New("ambiguous original business contact update")
	}
	if source.BusinessConnection != nil {
		return true, s.saveBusinessContactConnection(ctx, source)
	}
	message := source.BusinessMessage
	if message == nil {
		message = source.EditedBusinessMessage
	}
	if s.cfg.SourceBotID <= 0 || !businessConnectionID(message.BusinessConnectionID) || message.ID <= 0 || message.Date <= 0 || message.From.ID != s.cfg.SourceBotID || !message.From.IsBot || message.Chat.ID != s.cfg.SourceBotID || message.Chat.Type != "private" || message.ForwardOrigin != nil || message.SenderBusinessBot != nil {
		return true, nil
	}
	profile, contactURL, profileURL := businessContactURLs(message.ReplyMarkup)
	return true, s.saveBusinessContactMessage(ctx, source, message, profile, contactURL, profileURL)
}

func (s *Store) businessSession(ctx context.Context, tx *sql.Tx, owner int64) (Session, error) {
	var session Session
	var encrypted []byte
	var upstream int64
	var state string
	if err := tx.QueryRowContext(ctx, "SELECT encrypted,upstream_id,state FROM sessions WHERE owner_id=?", owner).Scan(&encrypted, &upstream, &state); err != nil {
		return session, err
	}
	raw, err := unseal(s.cfg.SessionKey, encrypted, sessionPurpose(owner))
	if err != nil {
		return session, errors.New("business contact session decryption failed")
	}
	if err = json.Unmarshal(raw, &session); err != nil || session.UserID != upstream {
		return session, errors.New("invalid business contact session binding")
	}
	session.State = state
	return session, nil
}

func (s *Store) saveBusinessContactConnection(ctx context.Context, update Update) error {
	connection := update.BusinessConnection
	if !businessConnectionID(connection.ID) || connection.User.ID <= 0 || connection.User.IsBot || connection.UserChatID <= 0 || connection.Date <= 0 {
		return nil
	}
	mu := s.SessionMutex(connection.User.ID)
	mu.Lock()
	defer mu.Unlock()
	if err := s.initBusinessContacts(ctx); err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='business_contact_connections'"); err != nil {
		return err
	}
	var known bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM users WHERE id=?)", connection.User.ID).Scan(&known); err != nil || !known {
		return err
	}
	var owner, upstream, last int64
	var previousRaw string
	err = tx.QueryRowContext(ctx, "SELECT owner_id,upstream_id,update_id,raw_json FROM business_contact_connections WHERE id=?", connection.ID).Scan(&owner, &upstream, &last, &previousRaw)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil {
		if owner != connection.User.ID {
			return errors.New("business contact connection owner conflict")
		}
		if update.ID < last {
			return nil
		}
		if update.ID == last {
			if previousRaw != string(update.RawJSON) {
				return ErrContactEvidenceConflict
			}
			return nil
		}
	}
	if !connection.IsEnabled {
		if owner == 0 {
			return nil
		}
		_, err = tx.ExecContext(ctx, "UPDATE business_contact_connections SET enabled=0,update_id=?,raw_json=? WHERE id=?", update.ID, string(update.RawJSON), connection.ID)
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	var newest int64
	if err = tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(update_id),0) FROM business_contact_connections WHERE owner_id=?", connection.User.ID).Scan(&newest); err != nil || update.ID <= newest {
		return err
	}
	session, err := s.businessSession(ctx, tx, connection.User.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if session.UserID <= 0 || session.State != "connected" && session.State != "paused" {
		return nil
	}
	if owner != 0 && upstream != session.UserID {
		return nil
	}
	if _, err = tx.ExecContext(ctx, "UPDATE business_contact_connections SET enabled=0 WHERE owner_id=?", connection.User.ID); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO business_contact_connections(id,owner_id,upstream_id,session_generation,user_chat_id,connection_date,enabled,update_id,raw_json)
 VALUES(?,?,?,?,?,?,1,?,?) ON CONFLICT(id) DO UPDATE SET session_generation=excluded.session_generation,user_chat_id=excluded.user_chat_id,connection_date=excluded.connection_date,enabled=1,update_id=excluded.update_id,raw_json=excluded.raw_json`, connection.ID, connection.User.ID, session.UserID, session.Generation, connection.UserChatID, connection.Date, update.ID, string(update.RawJSON))
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) saveBusinessContactMessage(ctx context.Context, update Update, message *Message, profile int64, contactURL, profileURL string) error {
	if err := s.initBusinessContacts(ctx); err != nil {
		return err
	}
	var owner int64
	err := s.DB.QueryRowContext(ctx, "SELECT owner_id FROM business_contact_connections WHERE id=?", message.BusinessConnectionID).Scan(&owner)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	mu := s.SessionMutex(owner)
	mu.Lock()
	defer mu.Unlock()
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key='profile_business_contacts'"); err != nil {
		return err
	}
	var upstream, date, last int64
	var generation string
	var enabled bool
	if err = tx.QueryRowContext(ctx, "SELECT upstream_id,session_generation,connection_date,enabled,update_id FROM business_contact_connections WHERE id=? AND owner_id=?", message.BusinessConnectionID, owner).Scan(&upstream, &generation, &date, &enabled, &last); errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	timestamp := message.Date
	if update.EditedBusinessMessage != nil {
		if message.EditDate > 0 && message.EditDate >= message.Date {
			timestamp = message.EditDate
		} else {
			profile = 0
		}
	}
	if !enabled || timestamp < date || update.ID <= last {
		return nil
	}
	session, err := s.businessSession(ctx, tx, owner)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if session.UserID != upstream || session.Generation != generation || session.State != "connected" && session.State != "paused" && session.State != "reconnect_required" {
		return nil
	}
	var previousRaw string
	err = tx.QueryRowContext(ctx, "SELECT raw_json FROM business_contact_inbox WHERE update_id=?", update.ID).Scan(&previousRaw)
	if err == nil {
		if previousRaw != string(update.RawJSON) {
			return ErrContactEvidenceConflict
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	legacyContact := false
	err = tx.QueryRowContext(ctx, "SELECT raw_json FROM profile_business_contacts WHERE update_id=?", update.ID).Scan(&previousRaw)
	if err == nil {
		if previousRaw != string(update.RawJSON) {
			return ErrContactEvidenceConflict
		}
		legacyContact = true
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	parseState := "unresolved"
	if profile > 0 || legacyContact {
		parseState = "contact"
	}
	now := time.Now().UTC()
	_, err = tx.ExecContext(ctx, `INSERT INTO business_contact_inbox(owner_id,upstream_id,source_bot_id,connection_id,source_message_id,source_message_timestamp,update_id,captured_at_ms,parse_state,raw_json)
 VALUES(?,?,?,?,?,?,?,?,?,?)`, owner, upstream, s.cfg.SourceBotID, message.BusinessConnectionID, message.ID, timestamp, update.ID, now.UnixMilli(), parseState, string(update.RawJSON))
	if err != nil {
		return err
	}
	if profile == 0 || legacyContact {
		return tx.Commit()
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO profile_business_contacts(owner_id,upstream_id,profile_id,source_bot_id,connection_id,source_message_id,source_message_timestamp,update_id,observed_at_ms,contact_url,profile_url,raw_json)
 VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, owner, upstream, profile, s.cfg.SourceBotID, message.BusinessConnectionID, message.ID, timestamp, update.ID, now.UnixMilli(), contactURL, profileURL, string(update.RawJSON))
	if err != nil {
		return err
	}
	if err = s.receiveContactJobTx(ctx, tx, owner, upstream, profile, now); err != nil {
		return err
	}
	return tx.Commit()
}

func businessContactURLs(raw json.RawMessage) (int64, string, string) {
	var markup struct {
		Rows [][]struct {
			URL    string `json:"url"`
			WebApp *struct {
				URL string `json:"url"`
			} `json:"web_app"`
		} `json:"inline_keyboard"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &markup) != nil {
		return 0, "", ""
	}
	contacts := map[string]bool{}
	profiles := map[string]int64{}
	for _, row := range markup.Rows {
		for _, button := range row {
			urls := []string{button.URL}
			if button.WebApp != nil {
				urls = append(urls, button.WebApp.URL)
			}
			for _, rawURL := range urls {
				if validBusinessTelegramURL(rawURL) {
					contacts[rawURL] = true
				}
				if profile := businessProfileTarget(rawURL); profile > 0 {
					profiles[rawURL] = profile
				}
			}
		}
	}
	if len(contacts) != 1 || len(profiles) != 1 {
		return 0, "", ""
	}
	var contact string
	for value := range contacts {
		contact = value
	}
	for rawURL, profile := range profiles {
		return profile, contact, rawURL
	}
	return 0, "", ""
}

func validBusinessTelegramURL(raw string) bool {
	u, err := contactEvidenceURL(raw, "t.me", "")
	if err != nil || u.RawQuery != "" || u.ForceQuery || !strings.HasPrefix(u.Path, "/") {
		return false
	}
	username := strings.TrimPrefix(u.Path, "/")
	if len(username) < 5 || len(username) > 32 {
		return false
	}
	for i, char := range username {
		letter := char >= 'A' && char <= 'Z' || char >= 'a' && char <= 'z'
		if !letter && (i == 0 || char < '0' || char > '9') && (i == 0 || char != '_') {
			return false
		}
	}
	return true
}

func businessProfileTarget(raw string) int64 {
	u, err := contactEvidenceURL(raw, "mini.jardulik.app", "/mutuals")
	if err != nil {
		return 0
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil || len(query) != 2 || len(query["targetUserId"]) != 1 || len(query["noCheck"]) != 1 || query.Get("noCheck") != "1" {
		return 0
	}
	id, err := strconv.ParseInt(query.Get("targetUserId"), 10, 64)
	if err != nil || id <= 0 || strconv.FormatInt(id, 10) != query.Get("targetUserId") {
		return 0
	}
	return id
}

func (s *Store) businessTelegramContact(ctx context.Context, owner, profile int64) (*ContactEvidence, error) {
	if err := s.initBusinessContacts(ctx); err != nil {
		return nil, err
	}
	result := ContactEvidence{OwnerID: owner, EvidenceKind: "telegram_business_message"}
	var connection string
	var message, observed int64
	err := s.DB.QueryRowContext(ctx, `SELECT e.id,e.upstream_id,e.profile_id,e.source_bot_id,e.connection_id,e.source_message_id,e.source_message_timestamp,e.observed_at_ms,e.contact_url,e.profile_url
 FROM profile_business_contacts e JOIN sessions s ON s.owner_id=e.owner_id AND s.upstream_id=e.upstream_id
 WHERE e.owner_id=? AND e.profile_id=? AND e.source_bot_id=? AND s.state IN ('connected','paused','reconnect_required')
 ORDER BY e.source_message_timestamp DESC,e.update_id DESC LIMIT 1`, owner, profile, s.cfg.SourceBotID).Scan(&result.ID, &result.UpstreamID, &result.ProfileID, &result.SourceBotID, &connection, &message, &result.SourceMessageTimestamp, &observed, &result.ContactURL, &result.ProfileURL)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	result.SourcePeerID = result.SourceBotID
	result.SourceLocatorKind = "bot_api_message"
	result.SourceMessageLocator = fmt.Sprintf("business:%s:%d", connection, message)
	result.ObservedAt = time.UnixMilli(observed).UTC()
	// InputJSON is a projection for existing callers. raw_json retains the exact source update.
	body, err := json.Marshal(result.ContactEvidenceInput)
	if err != nil {
		return nil, err
	}
	result.InputJSON = string(body)
	return &result, nil
}

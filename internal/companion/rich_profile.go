package companion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"
)

var ErrRichCardTooLarge = errors.New("rich card exceeds Telegram limits")
var ErrRichCardMarker = errors.New("invalid rich card marker")

type RichProfileCard struct {
	Content                   map[string]any
	Photos                    []string
	TextBytes, TextCharacters int
	JSONBytes, JSONCharacters int
}

func BuildRichProfileCard(p StoredProfile, jobs []MediaJob, postLink string) (RichProfileCard, error) {
	var card RichProfileCard
	if p.ID <= 0 || p.VersionID <= 0 {
		return card, errors.New("invalid rich profile identity")
	}
	var pretty bytes.Buffer
	if json.Indent(&pretty, []byte(p.RawJSON), "", "  ") != nil {
		return card, errors.New("invalid source profile JSON")
	}
	card.JSONBytes, card.JSONCharacters = pretty.Len(), utf8.RuneCount(pretty.Bytes())
	photos := selectedProfilePhotos(jobs)
	if len(photos) > 50 {
		return card, fmt.Errorf("%w: media count", ErrRichCardTooLarge)
	}
	body, details, footer, link := splitPhotoFooter(profileText(p))
	if validPostURL(postLink) {
		link = postLink
	}
	richBody := richProfileBody(body, link)
	caption := map[string]any{"text": richBody}
	var media []any
	for i, photo := range photos {
		if photo.ProfileID != p.ID || photo.VersionID != p.VersionID {
			return card, errors.New("rich photo provenance mismatch")
		}
		if photo.Size > 9<<20 {
			return card, fmt.Errorf("%w: photo size", ErrRichCardTooLarge)
		}
		info, err := os.Lstat(photo.Path)
		if err != nil || !info.Mode().IsRegular() || photo.Size <= 0 || info.Size() != photo.Size {
			return card, errors.New("rich photo file unavailable")
		}
		card.Photos = append(card.Photos, photo.Path)
		media = append(media, map[string]any{"type": "photo", "photo": map[string]any{"type": "photo", "media": fmt.Sprintf("attach://p%d", i)}, "caption": caption})
	}
	fold := profileQuoteText(strings.Join(photoMetadataRows(photos), "\n"), details)
	if fold != "" {
		fold += "\n"
	}
	var blocks []any
	if len(media) == 1 {
		blocks = append(blocks, media[0])
	} else if len(media) > 1 {
		// PhotoViewer reads each photo caption; the collage caption shows the body once in chat.
		blocks = append(blocks, map[string]any{"type": "collage", "blocks": media, "caption": caption})
	} else {
		blocks = append(blocks, map[string]any{"type": "paragraph", "text": richBody})
	}
	idLine := fmt.Sprintf("Jardulik: %d", p.ID)
	idText := strconv.FormatInt(p.ID, 10)
	remaining := strings.TrimPrefix(fold, idLine)
	if remaining == fold {
		return card, errors.New("rich profile ID details mismatch")
	}
	blocks = append(blocks, map[string]any{"type": "expandable_blockquote", "text": []any{
		"Jardulik: ",
		map[string]any{"type": "button", "button": map[string]any{"text": idText, "copy_text": map[string]any{"text": idText}}},
		remaining,
		map[string]any{"type": "code", "text": pretty.String()},
	}})
	footer = strings.TrimSpace(footer)
	footerText := footer
	if footerText != "" {
		blocks = append(blocks, map[string]any{"type": "footer", "text": footerText})
	}
	bodyCopies := 1
	if len(media) > 1 {
		bodyCopies += len(media)
	}
	otherText := fold + pretty.String() + footerText
	card.TextBytes = bodyCopies*len(body) + len(otherText)
	card.TextCharacters = bodyCopies*utf8.RuneCountInString(body) + utf8.RuneCountInString(otherText)
	if card.TextCharacters > 32768 {
		return card, fmt.Errorf("%w: text length", ErrRichCardTooLarge)
	}
	card.Content = map[string]any{"skip_entity_detection": true, "blocks": blocks}
	return card, nil
}

func richProfileBody(body, link string) any {
	if !validPostURL(link) {
		return body
	}
	heading, rest, hasRest := strings.Cut(body, "\n")
	parts := []any{map[string]any{"type": "url", "text": heading, "url": link}}
	if hasRest {
		parts = append(parts, "\n"+rest)
	}
	return parts
}

func validRichPhotoFileID(value string) bool {
	return value != "" && value == strings.TrimSpace(value) && !strings.Contains(value, "://") && !strings.HasPrefix(value, "attach:")
}

// Returns a copy for editMessageText; no attach references or uploads remain.
func (card RichProfileCard) WithPhotoFileIDs(refs []string) (map[string]any, error) {
	if len(refs) != len(card.Photos) {
		return nil, errors.New("rich photo receipt count changed")
	}
	for _, ref := range refs {
		if !validRichPhotoFileID(ref) {
			return nil, errors.New("invalid rich photo receipt")
		}
	}
	raw, err := json.Marshal(card.Content)
	if err != nil {
		return nil, err
	}
	var content map[string]any
	if err = json.Unmarshal(raw, &content); err != nil {
		return nil, err
	}
	index := 0
	var walk func(any) error
	walk = func(value any) error {
		block, ok := value.(map[string]any)
		if !ok {
			return errors.New("invalid rich block")
		}
		if block["type"] == "photo" {
			photo, ok := block["photo"].(map[string]any)
			if !ok || index >= len(refs) || photo["media"] != fmt.Sprintf("attach://p%d", index) {
				return errors.New("rich photo order changed")
			}
			photo["media"] = refs[index]
			index++
		}
		if children, ok := block["blocks"].([]any); ok {
			for _, child := range children {
				if err := walk(child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	blocks, ok := content["blocks"].([]any)
	if !ok {
		return nil, errors.New("invalid rich content")
	}
	for _, block := range blocks {
		if err = walk(block); err != nil {
			return nil, err
		}
	}
	if index != len(refs) {
		return nil, errors.New("rich photo receipt count mismatch")
	}
	return content, nil
}

// RichBlockPhoto contains the confirmed PhotoSize vector, in rendered block order.
func richPhotoFileIDs(raw json.RawMessage) ([]string, error) {
	var root struct {
		Blocks json.RawMessage `json:"blocks"`
	}
	if json.Unmarshal(raw, &root) != nil || len(bytes.TrimSpace(root.Blocks)) == 0 || bytes.TrimSpace(root.Blocks)[0] != '[' {
		return nil, errors.New("invalid rich receipt")
	}
	var refs []string
	nodes := 0
	var walk func(json.RawMessage, int) error
	walk = func(raw json.RawMessage, depth int) error {
		nodes++
		if nodes > 500 || depth > 16 {
			return errors.New("rich receipt limits exceeded")
		}
		var block struct {
			Type   string            `json:"type"`
			Blocks []json.RawMessage `json:"blocks"`
			Photo  []struct {
				FileID        string `json:"file_id"`
				Width, Height int64
			} `json:"photo"`
		}
		if json.Unmarshal(raw, &block) != nil {
			return errors.New("invalid rich receipt block")
		}
		if block.Type == "photo" {
			best := ""
			var area int64
			for _, photo := range block.Photo {
				if !validRichPhotoFileID(photo.FileID) || photo.Width <= 0 || photo.Height <= 0 || photo.Width > 65535 || photo.Height > 65535 {
					return errors.New("invalid rich receipt photo")
				}
				if candidate := photo.Width * photo.Height; best == "" || candidate > area {
					best, area = photo.FileID, candidate
				}
			}
			if best == "" {
				return errors.New("missing rich receipt photo")
			}
			refs = append(refs, best)
			if len(refs) > 50 {
				return errors.New("rich receipt media limit exceeded")
			}
		}
		for _, child := range block.Blocks {
			if err := walk(child, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	var blocks []json.RawMessage
	if json.Unmarshal(root.Blocks, &blocks) != nil {
		return nil, errors.New("invalid rich receipt blocks")
	}
	for _, block := range blocks {
		if err := walk(block, 1); err != nil {
			return nil, err
		}
	}
	return refs, nil
}

type richCardMarker struct {
	OwnerID      int64    `json:"owner_id"`
	VersionID    int64    `json:"version_id"`
	ThreadID     int64    `json:"thread_id"`
	PhotoFileIDs []string `json:"photo_file_ids,omitempty"`
}

func richCardKey(chat, message int64) string {
	return "rich_card:" + strconv.FormatInt(chat, 10) + ":" + strconv.FormatInt(message, 10)
}
func validRichCardMarker(marker richCardMarker) bool {
	if marker.OwnerID <= 0 || marker.VersionID <= 0 || marker.ThreadID < 0 || len(marker.PhotoFileIDs) > 50 {
		return false
	}
	for _, ref := range marker.PhotoFileIDs {
		if !validRichPhotoFileID(ref) {
			return false
		}
	}
	return true
}
func loadRichCardMarker(ctx context.Context, store *Store, chat, message int64) (*richCardMarker, error) {
	if chat == 0 || message <= 0 {
		return nil, fmt.Errorf("%w: target", ErrRichCardMarker)
	}
	var raw string
	err := store.DB.QueryRowContext(ctx, "SELECT value FROM meta WHERE key=?", richCardKey(chat, message)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var marker richCardMarker
	if json.Unmarshal([]byte(raw), &marker) != nil || !validRichCardMarker(marker) {
		return nil, ErrRichCardMarker
	}
	var exists bool
	if err = store.DB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM versions WHERE owner_id=? AND id=?)", marker.OwnerID, marker.VersionID).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("%w: source unavailable", ErrRichCardMarker)
	}
	return &marker, nil
}
func recordRichCard(ctx context.Context, store *Store, chat, message, owner, version, thread int64, rich ...json.RawMessage) error {
	if chat == 0 || message <= 0 || len(rich) != 1 {
		return errors.New("invalid confirmed rich marker")
	}
	refs, err := richPhotoFileIDs(rich[0])
	if err != nil {
		return err
	}
	marker := richCardMarker{OwnerID: owner, VersionID: version, ThreadID: thread, PhotoFileIDs: refs}
	if !validRichCardMarker(marker) {
		return errors.New("invalid rich marker provenance")
	}
	encoded, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	tx, err := store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE meta SET value=value WHERE key=?", richCardKey(chat, message)); err != nil {
		return err
	}
	var exists bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM versions WHERE owner_id=? AND id=?)", owner, version).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return errors.New("rich marker source unavailable")
	}
	var count int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM media_jobs j WHERE j.owner_id=? AND j.version_id=? AND j.state='complete' AND COALESCE(j.sha,'')<>'' AND j.role=CASE WHEN EXISTS(SELECT 1 FROM media_jobs f WHERE f.owner_id=? AND f.version_id=? AND f.state='complete' AND COALESCE(f.sha,'')<>'' AND f.role='photos') THEN 'photos' ELSE 'miniPhotos' END`, owner, version, owner, version).Scan(&count); err != nil {
		return err
	}
	if count != len(refs) {
		return errors.New("rich marker photo count mismatch")
	}
	var previous string
	err = tx.QueryRowContext(ctx, "SELECT value FROM meta WHERE key=?", richCardKey(chat, message)).Scan(&previous)
	if err == nil && previous != string(encoded) {
		return errors.New("rich marker conflict")
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO NOTHING", richCardKey(chat, message), string(encoded)); err != nil {
		return err
	}
	return tx.Commit()
}

package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const profileCatalogueTitle = "📚 Каталог"

type catalogueTitleState struct {
	State string `json:"state"`
	Next  int64  `json:"next_attempt,omitempty"`
	Error string `json:"error,omitempty"`
}

func catalogueTitleKey(t profileIndexTopic) string {
	return fmt.Sprintf("profile_catalogue_title:%d:%d:%d:%d", t.Owner, t.Upstream, t.Chat, t.Thread)
}

func readCatalogueTitle(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, t profileIndexTopic) (catalogueTitleState, error) {
	var state catalogueTitleState
	var raw string
	err := q.QueryRowContext(ctx, "SELECT value FROM meta WHERE key=?", catalogueTitleKey(t)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	if json.Unmarshal([]byte(raw), &state) != nil {
		return state, errors.New("invalid catalogue title receipt")
	}
	return state, nil
}

func writeCatalogueTitle(ctx context.Context, q interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, t profileIndexTopic, state catalogueTitleState) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", catalogueTitleKey(t), string(raw))
	return err
}

func cataloguePhysicalPage(kind string, page int) int {
	if kind == profileIndexAge {
		return 2 * page
	}
	return 2*page - 1
}

func (s *Service) catalogueTitleConfirmed(ctx context.Context, owner, upstream int64, generation string) (bool, error) {
	topic, err := scanProfileIndexTopic(s.Store.DB.QueryRowContext(ctx, "SELECT "+profileIndexTopicColumns+" FROM profile_index_topics WHERE owner_id=? AND upstream_id=? AND generation=? AND chat_id=? AND kind='compatibility' AND state='sent' AND thread_id>0", owner, upstream, generation, s.Config.BackupChatID))
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	state, err := readCatalogueTitle(ctx, s.Store.DB, topic)
	return state.State == "sent", err
}

// Retired slots keep their message IDs, dates, links and readable rows.
func catalogueLegacyPayload(old profileIndexPayload, kind, link string, rich bool) (profileIndexPayload, error) {
	if old.Legacy {
		return old, nil
	}
	parts := strings.SplitN(old.Text, "\n", 2)
	body := ""
	oldStart := textUnits(old.Text)
	if len(parts) == 2 {
		body = strings.TrimLeft(parts[1], "\n")
		oldStart = textUnits(parts[0]) + 1 + textUnits(parts[1]) - textUnits(body)
	}
	if body == "" {
		body = old.Text
		oldStart = 0
	}
	heading := profileIndexTitle(kind) + " · архив"
	payload := profileIndexPayload{Text: heading + "\n" + body, Legacy: true, Entities: []MessageEntity{{Type: "text_link", Length: textUnits(heading), URL: link}}}
	start := textUnits(heading) + 1
	for _, entity := range old.Entities {
		if entity.Type == "text_link" && entity.Offset >= oldStart && entity.Offset+entity.Length <= oldStart+textUnits(body) {
			entity.Offset += start - oldStart
			payload.Entities = append(payload.Entities, entity)
		}
	}
	payload.Entities = append(payload.Entities, MessageEntity{Type: "expandable_blockquote", Offset: start, Length: textUnits(body)})
	if rich || old.Rich != nil {
		payload.Rich = profileIndexRichContent(payload)
	}
	if err := validProfileIndexPayload(payload); err != nil {
		return profileIndexPayload{}, err
	}
	return payload, nil
}

func (s *Service) retireCataloguePagesTx(ctx context.Context, tx *sql.Tx, topic profileIndexTopic, active map[int]bool, link string) error {
	rows, err := tx.QueryContext(ctx, "SELECT page_no,message_id,state,payload_json FROM profile_index_pages WHERE topic_id=?", topic.ID)
	if err != nil {
		return err
	}
	type page struct {
		number  int
		message int64
		state   string
		raw     []byte
	}
	var pages []page
	for rows.Next() {
		var p page
		if err = rows.Scan(&p.number, &p.message, &p.state, &p.raw); err != nil {
			rows.Close()
			return err
		}
		pages = append(pages, p)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, p := range pages {
		if active[p.number] {
			continue
		}
		if p.message == 0 {
			if p.state != "uncertain" && p.state != "sending" && p.state != "preparing" {
				if _, err = tx.ExecContext(ctx, "UPDATE profile_index_pages SET state='error',error='unpublished_page_superseded',updated_at=CURRENT_TIMESTAMP WHERE topic_id=? AND page_no=? AND message_id=0", topic.ID, p.number); err != nil {
					return err
				}
			}
			continue
		}
		var old profileIndexPayload
		if json.Unmarshal(p.raw, &old) != nil {
			return errors.New("invalid retained catalogue payload")
		}
		payload, err := catalogueLegacyPayload(old, topic.Kind, link, s.Config.RichCards)
		if err != nil {
			return err
		}
		if err = s.stageProfileIndexPageTx(ctx, tx, topic.ID, p.number, payload); err != nil {
			return err
		}
	}
	return nil
}

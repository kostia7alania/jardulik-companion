package companion

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const archiveAgePageSize = 20

type ArchiveAgePage struct {
	Text       string
	Entities   []MessageEntity
	Offset     int
	Total      int
	NextOffset int
	HasMore    bool
}

// Birthdays are source calendar days, not instants or rounded age values.
func profileAgeOrderKey(raw any) string {
	if profileBirthday(Profile{"birthday": raw}) == "" {
		return ""
	}
	return strings.TrimSpace(raw.(string))[:10]
}

// ArchiveAgeIndex reads existing owner-scoped profiles and topics without changing either.
func (s *Store) ArchiveAgeIndex(ctx context.Context, owner int64, offset int) (ArchiveAgePage, error) {
	page := ArchiveAgePage{Offset: offset}
	if owner <= 0 || offset < 0 {
		return page, errors.New("invalid age index page")
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT q.profile_id,mt.thread_id,
 json_extract(v.raw_json,'$.birthday'),json_extract(v.raw_json,'$.name'),json_extract(v.raw_json,'$.surName'),
 json_extract(v.raw_json,'$.age'),json_extract(v.raw_json,'$.height')
 FROM queue q JOIN versions v ON v.id=q.version_id AND v.owner_id=q.owner_id AND v.profile_id=q.profile_id
 JOIN mirror_topics mt ON mt.profile_id=q.profile_id AND mt.thread_id>0 WHERE q.owner_id=?`, owner)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	type entry struct {
		id, thread int64
		birthday   string
		heading    string
	}
	var entries []entry
	for rows.Next() {
		var item entry
		var birthday, name, surname, age, height any
		if err := rows.Scan(&item.id, &item.thread, &birthday, &name, &surname, &age, &height); err != nil {
			return page, err
		}
		item.birthday = profileAgeOrderKey(birthday)
		label := Profile{}
		for key, raw := range map[string]any{"name": name, "surName": surname} {
			if value, ok := raw.(string); ok {
				label[key] = value
			}
		}
		for key, raw := range map[string]any{"age": age, "height": height} {
			if n, ok := autoInteger(raw); ok && n > 0 {
				label[key] = strconv.FormatInt(n, 10)
			}
		}
		if strings.TrimSpace(label.String("name")+label.String("surName")) == "" {
			label["name"] = "Анкета"
		}
		label["name"], _ = telegramPrefix(strings.Join(strings.Fields(label.String("name")+" "+label.String("surName")), " "), 80)
		delete(label, "surName")
		item.heading = profileHeading(label)
		entries = append(entries, item)
	}
	if err := rows.Err(); err != nil {
		return page, err
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].birthday != entries[j].birthday {
			return entries[i].birthday > entries[j].birthday
		}
		return entries[i].id < entries[j].id
	})
	page.Total = len(entries)
	if offset >= page.Total {
		page.NextOffset = page.Total
		page.Text = "Анкет в этом разделе пока нет."
		return page, nil
	}
	end := page.Total
	if page.Total-offset > archiveAgePageSize {
		end = offset + archiveAgePageSize
	}
	page.NextOffset, page.HasMore = end, end < page.Total
	var text strings.Builder
	fmt.Fprintf(&text, "Архив по возрасту: %d-%d из %d", offset+1, end, page.Total)
	for i, item := range entries[offset:end] {
		link := forumTopicURL(s.cfg.BackupChatID, item.thread)
		if link == "" {
			return ArchiveAgePage{}, errors.New("архивная группа не настроена")
		}
		fmt.Fprintf(&text, "\n%d. ", offset+i+1)
		start := textUnits(text.String())
		label := fmt.Sprintf("%s · #%d", item.heading, item.id)
		text.WriteString(label)
		page.Entities = append(page.Entities, MessageEntity{Type: "text_link", Offset: start, Length: textUnits(label), URL: link})
	}
	page.Text = text.String()
	return page, nil
}

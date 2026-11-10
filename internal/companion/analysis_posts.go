package companion

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const analysisPostsSchema = `CREATE TABLE IF NOT EXISTS analysis_posts (
 analysis_id INTEGER PRIMARY KEY REFERENCES profile_analyses(id) ON DELETE CASCADE,
 owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 upstream_id INTEGER NOT NULL, profile_id INTEGER NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('pending','preparing','sending','sent','error','uncertain')),
 chat_id INTEGER NOT NULL DEFAULT 0, thread_id INTEGER NOT NULL DEFAULT 0,
 message_id INTEGER NOT NULL DEFAULT 0, next_attempt INTEGER NOT NULL DEFAULT 0,
 payload_json BLOB NOT NULL DEFAULT X'', receipt_json BLOB NOT NULL DEFAULT X'',
 response_sha TEXT NOT NULL DEFAULT '', error TEXT NOT NULL DEFAULT '',
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS analysis_posts_pending ON analysis_posts(state,next_attempt,analysis_id);`

// Wait for the current archive card/layout and any older queued archive versions.
const analysisPostReady = `EXISTS(
 SELECT 1 FROM queue q JOIN archive_versions av ON av.version_id=q.version_id AND av.owner_id=q.owner_id AND av.profile_id=q.profile_id
 WHERE q.owner_id=p.owner_id AND q.profile_id=p.profile_id AND av.thread_id=t.thread_id AND (
  (av.state IN ('sent','partial') AND EXISTS(SELECT 1 FROM archive_profile_repairs layout WHERE layout.version_id=av.version_id AND layout.state='sent'))
  OR (av.state='duplicate' AND EXISTS(SELECT 1 FROM archive_versions original JOIN archive_profile_repairs layout ON layout.version_id=original.version_id
      WHERE original.fingerprint=av.fingerprint AND original.profile_id=av.profile_id AND original.thread_id=t.thread_id AND original.state IN ('sent','partial') AND layout.state='sent'))
 )) AND NOT EXISTS(
 SELECT 1 FROM versions v LEFT JOIN archive_versions av ON av.version_id=v.id
 WHERE v.owner_id=p.owner_id AND v.profile_id=p.profile_id AND (
  av.version_id IS NULL OR av.state IN ('pending','processing')
  OR (av.state IN ('sent','partial') AND NOT EXISTS(SELECT 1 FROM archive_profile_repairs layout WHERE layout.version_id=v.id AND layout.state='sent'))
 ))`

func analysisPostPresenceGuard(ctx context.Context, store *Store) (string, error) {
	guard := ""
	for _, check := range []struct{ table, clause string }{
		{"profile_presence", " AND NOT EXISTS(SELECT 1 FROM profile_presence pp WHERE pp.owner_id=p.owner_id AND pp.profile_id=p.profile_id AND pp.availability='unavailable')"},
		{"profile_presence_events", " AND NOT EXISTS(SELECT 1 FROM profile_presence_events pe WHERE pe.owner_id=p.owner_id AND pe.profile_id=p.profile_id AND pe.state IN ('pending','processing'))"},
	} {
		exists, err := snapshotHasTable(ctx, store.DB, check.table)
		if err != nil {
			return "", err
		}
		if exists {
			guard += check.clause
		}
	}
	return guard, nil
}

func analysisPostSuccessSQL(alias string) string {
	return alias + `.http_status=200 AND ` + alias + `.state='response_received' AND CASE WHEN json_valid(CAST(` + alias + `.response_body AS TEXT)) THEN
  json_type(CAST(` + alias + `.response_body AS TEXT),'$.content')='text' AND length(trim(json_extract(CAST(` + alias + `.response_body AS TEXT),'$.content'),char(9)||char(10)||char(13)||' '))>0 ELSE 0 END`
}

type analysisPostPayload struct {
	Text     string          `json:"text"`
	Entities []MessageEntity `json:"entities"`
	Keyboard any             `json:"reply_markup,omitempty"`
}

var analysisPostRatingPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(?:оценка[\s*_]+совместимости[\s*_]*(?::[\s*_]*|[—–-][\s*_]+)?|совместимость[\s*_]*(?::[\s*_]*|[—–-][\s*_]+))([0-9]{1,3})[\s*_]*%`),
	regexp.MustCompile(`(?i)совместимость[\s*_]+можно[\s*_]+оценить[\s*_]+в[\s*_]+([0-9]{1,3})[\s*_]*%`),
	regexp.MustCompile(`(?i)(?:я[\s*_]+)?оцениваю[\s*_]+вашу[\s*_]+совместимость[\s*_]+на[\s*_]+([0-9]{1,3})[\s*_]*%`),
}
var analysisPostRatingRange = regexp.MustCompile(`(?i)^[\s*_]*(?:[-–—/±…]|\.{2,}|(?:до|или|и)[\s*_]+)[\s*_]*[0-9]`)

func analysisPostRatingWords(text string) []string {
	return strings.FieldsFunc(strings.ToLower(text), func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) })
}

var analysisPostRatingListPrefix = regexp.MustCompile(`^(?:[0-9]+[.)][\t ]*|[-+*][\t ]+)`)

func analysisPostOverallHeading(content string, before int) bool {
	overall, _ := analysisPostOverallHeadingStatus(content, before)
	return overall
}

// The second result distinguishes an explicit heading from the legacy no-heading default.
func analysisPostOverallHeadingStatus(content string, before int) (bool, bool) {
	previous := content[:before]
	for previous != "" {
		previous = strings.TrimRight(previous, " \t\r\n")
		start := strings.LastIndex(previous, "\n") + 1
		line := strings.TrimSpace(previous[start:])
		previous = previous[:start]
		markdown := strings.HasPrefix(line, "#")
		if markdown {
			line = strings.TrimSpace(strings.TrimLeft(line, "#"))
		}
		numbered := analysisPostRatingListPrefix.MatchString(line)
		line = analysisPostRatingListPrefix.ReplaceAllString(line, "")
		heading := markdown || numbered || strings.HasSuffix(line, ":")
		for _, marker := range []string{"**", "__"} {
			if strings.HasPrefix(line, marker) {
				if close := strings.Index(line[2:], marker); close >= 0 {
					line = line[2 : close+2]
					heading = true
				}
				break
			}
		}
		words := analysisPostRatingWords(line)
		for len(words) > 0 {
			if _, err := strconv.Atoi(words[0]); err != nil {
				break
			}
			words = words[1:]
		}
		switch strings.Join(words, " ") {
		case "итог", "заключение", "вывод", "выводы", "итоговая оценка", "итоговая совместимость", "общая оценка", "общая совместимость", "общая оценка совместимости", "итоговая оценка совместимости", "оценка совместимости", "в целом":
			return true, true
		default:
			if heading {
				return false, true
			}
		}
	}
	return true, false
}

// Explicit overall forms are accepted only in an overall sentence/heading.
// A category heading or a negated/ranged statement must not become a display score.
func analysisPostRatingContext(content string, start, end int) (eligible, denied bool) {
	if start > 0 {
		before, _ := utf8.DecodeLastRuneInString(content[:start])
		if unicode.IsLetter(before) || unicode.IsDigit(before) {
			return false, false
		}
	}
	lineStart := strings.LastIndex(content[:start], "\n") + 1
	prefixStart := lineStart
	if boundary := strings.LastIndexAny(content[lineStart:start], ".!?;"); boundary >= 0 {
		prefixStart = lineStart + boundary + 1
	}
	prefix := analysisPostRatingWords(content[prefixStart:start])
	overall := false
	for _, word := range prefix {
		switch word {
		case "не", "нет", "нельзя", "отрицаю":
			return false, true
		case "итог", "итоговая", "итоговую", "общая", "общую", "суммарная":
			overall = true
		}
	}
	for _, word := range prefix {
		if _, err := strconv.Atoi(word); err == nil {
			continue
		}
		switch word {
		case "итог", "итоговая", "итоговую", "общая", "общую", "суммарная", "ваша", "вашу", "наша", "оценка", "в", "целом", "я":
		default:
			return false, false
		}
	}
	for i := 1; i < len(prefix); i++ {
		if prefix[i-1] == "в" && prefix[i] == "целом" {
			overall = true
		}
	}
	// Include a heading before a sentence separator on the current line.
	if !overall && !analysisPostOverallHeading(content, start) {
		return false, false
	}
	suffix := content[end:]
	if analysisPostRatingRange.MatchString(suffix) {
		return false, true
	}
	if boundary := strings.IndexAny(suffix, ".!?;"); boundary >= 0 {
		suffix = suffix[:boundary]
	}
	if paragraph := strings.Index(suffix, "\n\n"); paragraph >= 0 {
		suffix = suffix[:paragraph]
	}
	words := analysisPostRatingWords(suffix)
	for _, word := range words[:min(3, len(words))] {
		if word == "не" || word == "нет" || word == "нельзя" {
			return false, true
		}
	}
	// A qualifier after the percentage ("по ...") describes a partial dimension.
	if len(words) > 0 && words[0] == "по" {
		return false, false
	}
	return true, false
}

// This is a display quote from the provider, not a normalized compatibility score.
func analysisPostRating(content string) (int, bool) {
	value := -1
	for _, pattern := range analysisPostRatingPatterns {
		for _, match := range pattern.FindAllStringSubmatchIndex(content, -1) {
			eligible, denied := analysisPostRatingContext(content, match[0], match[1])
			if denied {
				return 0, false
			}
			if !eligible {
				continue
			}
			n, err := strconv.Atoi(content[match[2]:match[3]])
			if err != nil || n > 100 {
				return 0, false
			}
			if value >= 0 && value != n {
				return 0, false
			}
			value = n
		}
	}
	if value < 0 {
		if n, known := analysisPostFinalRating(content); known {
			return n, true
		}
		return analysisPostProviderRating(content)
	}
	// Newly understood final prose must not contradict a strict overall form.
	for _, match := range analysisPostAnyPercent.FindAllStringSubmatchIndex(content, -1) {
		if n, known := analysisPostFinalRatingAt(content, match); known && n != value {
			return 0, false
		}
	}
	return value, true
}

var analysisPostAnyPercent = regexp.MustCompile(`([0-9]+(?:[.,][0-9]+)?)[\s*_]*%`)

// A unique number under an explicit overall heading is a provider-reported
// overall value even when its prose uses an unsupported verb. Unlabelled prose
// or a category section never receives this fallback.
func analysisPostFinalRating(content string) (int, bool) {
	matches := analysisPostAnyPercent.FindAllStringSubmatchIndex(content, -1)
	if len(matches) != 1 {
		return 0, false
	}
	return analysisPostFinalRatingAt(content, matches[0])
}

func analysisPostFinalRatingAt(content string, match []int) (int, bool) {
	return analysisPostRatingAt(content, match, true)
}

// A single unambiguous percentage may also be quoted from the provider response
// without interpreting its prose as our own overall compatibility conclusion.
func analysisPostProviderRating(content string) (int, bool) {
	matches := analysisPostAnyPercent.FindAllStringSubmatchIndex(content, -1)
	if len(matches) != 1 {
		return 0, false
	}
	return analysisPostRatingAt(content, matches[0], false)
}

func analysisPostRatingAt(content string, match []int, requireOverall bool) (int, bool) {
	n, err := strconv.Atoi(content[match[2]:match[3]])
	if err != nil || n > 100 {
		return 0, false
	}
	if requireOverall {
		overall, found := analysisPostOverallHeadingStatus(content, match[0])
		if !found || !overall {
			return 0, false
		}
	}
	lineStart := strings.LastIndex(content[:match[0]], "\n") + 1
	if !requireOverall {
		// A soft line wrap must not sever a minus, range or negated clause.
		lineStart = strings.LastIndexAny(content[:match[0]], ".!?;") + 1
		if paragraph := strings.LastIndex(content[:match[0]], "\n\n"); paragraph >= lineStart {
			lineStart = paragraph + 2
		}
		if match[0] > 0 {
			before, _ := utf8.DecodeLastRuneInString(content[:match[0]])
			if before == '.' || before == ',' {
				return 0, false
			}
		}
	}
	prefix := content[lineStart:match[0]]
	if requireOverall {
		prefix = content[:match[0]]
	}
	untrimmedPrefix := prefix
	prefix = strings.TrimRight(prefix, " \t\r\n*_")
	if prefix != "" {
		before, size := utf8.DecodeLastRuneInString(prefix)
		if unicode.IsDigit(before) || before == '-' || before == '−' || before == '.' || before == ',' {
			return 0, false
		}
		if before == '–' || before == '—' {
			label := strings.TrimRight(prefix[:len(prefix)-size], " \t*_")
			last, _ := utf8.DecodeLastRuneInString(label)
			// A spaced dash after a text label is a separator; an adjacent
			// dash or one following a number remains a negative/range.
			if !unicode.IsLetter(last) || !strings.ContainsAny(untrimmedPrefix[len(prefix):], " \t") {
				return 0, false
			}
		}
	}
	if boundary := strings.LastIndexAny(content[lineStart:match[0]], ".!?;"); boundary >= 0 {
		lineStart += boundary + 1
	}
	words := analysisPostRatingWords(content[lineStart:match[0]])
	if requireOverall && !analysisPostOverallSubject(words) {
		return 0, false
	}
	for i, word := range words {
		switch word {
		case "между":
			// "Between you" names the pair, not numeric range endpoints.
			if i+1 >= len(words) || words[i+1] != "вами" {
				return 0, false
			}
			for _, following := range words[i+2:] {
				if following == "и" || strings.IndexFunc(following, unicode.IsDigit) >= 0 {
					return 0, false
				}
			}
		case "не", "нет", "нельзя", "отрицаю", "несовместимость", "например", "минус", "до", "или":
			return 0, false
		}
	}
	suffix := content[match[1]:]
	if analysisPostRatingRange.MatchString(suffix) {
		return 0, false
	}
	if boundary := strings.IndexAny(suffix, ".!?;"); boundary >= 0 {
		suffix = suffix[:boundary]
	}
	if paragraph := strings.Index(suffix, "\n\n"); paragraph >= 0 {
		suffix = suffix[:paragraph]
	}
	words = analysisPostRatingWords(suffix)
	for _, word := range words[:min(3, len(words))] {
		if word == "не" || word == "нет" || word == "нельзя" {
			return 0, false
		}
	}
	if requireOverall && len(words) > 0 && words[0] == "по" {
		return 0, false
	}
	return n, true
}

// Positive overall subjects are intentionally narrow; a heading alone cannot
// turn an interests/psychology percentage in ordinary prose into an overall score.
func analysisPostOverallSubject(words []string) bool {
	for i, word := range words {
		switch word {
		case "совместимость", "совместимости":
			if i+1 < len(words) && words[i+1] == "по" {
				return false
			}
			if i == 0 {
				return true
			}
			switch words[i-1] {
			case "ваша", "вашу", "вашей", "наша", "нашу", "нашей", "общая", "общую", "общей", "итоговая", "итоговую", "итоговой", "суммарная", "суммарную", "оценил", "оценить", "оцениваю", "оценим", "оценили", "оценивал":
				return true
			case "оценка", "оценку", "оценке", "оценки":
				if i == 1 {
					return true
				}
				switch words[i-2] {
				case "общая", "общую", "общей", "итоговая", "итоговую", "итоговой", "суммарная", "суммарную":
					return true
				}
			}
		case "оценка", "оценку", "оценке":
			if len(words) == 1 {
				return true
			}
			if i > 0 {
				switch words[i-1] {
				case "общая", "общую", "общей", "итоговая", "итоговую", "итоговой", "суммарная", "суммарную":
					return true
				}
			}
		case "итог":
			if len(words) == 1 {
				return true
			}
		}
	}
	return false
}

func analysisPostText(analysis ProfileAnalysis) (analysisPostPayload, error) {
	var payload analysisPostPayload
	own, err := debugProfile(analysis.OwnProfile)
	if err != nil {
		return payload, err
	}
	target, err := debugProfile(analysis.TargetProfile)
	if err != nil {
		return payload, err
	}
	ownAge, ownKnown := autoInteger(own["age"])
	targetAge, targetKnown := autoInteger(target["age"])
	if own.ID() != analysis.UpstreamID || target.ID() != analysis.ProfileID || !ownKnown || !targetKnown || ownAge < 18 || ownAge > 111 || targetAge < 18 || targetAge > 111 {
		return payload, errors.New("analysis post snapshot is not an eligible adult pair")
	}
	var response struct {
		Content string          `json:"content"`
		Model   json.RawMessage `json:"model"`
	}
	if analysis.HTTPStatus != 200 || analysis.State != "response_received" || analysis.ObservedAtMillis <= 0 || json.Unmarshal(analysis.ResponseBody, &response) != nil || strings.TrimSpace(response.Content) == "" {
		return payload, errors.New("analysis post has no successful GPT content")
	}
	model := analysisPostModel(analysis.Model, response.Model)
	title := "🧩 Совместимость"
	if percent, known := analysisPostRating(response.Content); known {
		title = fmt.Sprintf("🧩 Совместимость: %d%%", percent)
	}
	prefix := fmt.Sprintf("%s\n🤖 %s\n%s\n\n", title, model, displayTime(time.UnixMilli(analysis.ObservedAtMillis).UTC().Format(time.RFC3339Nano)))
	content, bold := analysisPostBold(response.Content)
	if textUnits(prefix+content) > 4096 {
		const tail = "\n\nПродолжение: полный JSON по кнопке."
		content, _ = telegramPrefix(content, 4096-textUnits(prefix+tail))
		payload.Text = prefix + content + tail
		payload.Keyboard = map[string]any{"inline_keyboard": [][]map[string]string{{{"text": "Полный расчёт", "callback_data": fmt.Sprintf("ga:%d", analysis.ID)}}}}
	} else {
		payload.Text = prefix + content
	}
	payload.Entities = append([]MessageEntity{{Type: "bold", Offset: 0, Length: textUnits(title)}}, analysisPostQuoteEntities(prefix, content, bold)...)
	return payload, validMessageEntities(payload.Text, payload.Entities)
}

func (s *Service) initAnalysisPosts(ctx context.Context) error {
	if _, err := s.Store.DB.ExecContext(ctx, analysisSchema); err != nil {
		return err
	}
	_, err := s.Store.DB.ExecContext(ctx, analysisPostsSchema)
	return err
}

func (s *Service) recoverAnalysisPosts(ctx context.Context) error {
	if err := s.initAnalysisPosts(ctx); err != nil {
		return err
	}
	tx, err := s.Store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE analysis_posts SET state='uncertain',error='worker_restarted_during_send',updated_at=CURRENT_TIMESTAMP WHERE owner_id=? AND state='sending'", s.Config.OwnerID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE analysis_posts SET state='pending',error='preparation_restarted',updated_at=CURRENT_TIMESTAMP WHERE owner_id=? AND state='preparing'", s.Config.OwnerID); err != nil {
		return err
	}
	return tx.Commit()
}

// This sender reads saved results only. It never calls Jardulik or creates topics.
func (s *Service) RunAnalysisPosts(ctx context.Context) {
	if s.Archiver == nil || s.Archiver.TG == nil || s.Config.OwnerID <= 0 || s.Config.OwnerID != s.Archiver.Config.OwnerID {
		return
	}
	if err := s.recoverAnalysisPosts(ctx); err != nil {
		slog.Error("analysis post recovery failed", "kind", safeError(err))
		return
	}
	for ctx.Err() == nil {
		if _, err := s.deliverAnalysisPost(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("analysis post unavailable", "kind", safeError(err))
		}
		if !sleep(ctx, 2*time.Second) {
			return
		}
	}
}

func (s *Service) seedAnalysisPosts(ctx context.Context) error {
	// Only the newest successful capture per directed pair enters historical backfill.
	_, err := s.Store.DB.ExecContext(ctx, `INSERT INTO analysis_posts(analysis_id,owner_id,upstream_id,profile_id,state,response_sha)
 SELECT a.id,a.owner_id,a.upstream_id,a.profile_id,'pending','' FROM profile_analyses a
 JOIN sessions se ON se.owner_id=a.owner_id AND se.upstream_id=a.upstream_id
 JOIN queue q ON q.owner_id=a.owner_id AND q.profile_id=a.profile_id
 WHERE a.owner_id=? AND `+analysisPostSuccessSQL("a")+`
 AND NOT EXISTS(SELECT 1 FROM profile_analyses newer WHERE newer.owner_id=a.owner_id AND newer.upstream_id=a.upstream_id AND newer.profile_id=a.profile_id
  AND `+analysisPostSuccessSQL("newer")+` AND (newer.observed_at_ms>a.observed_at_ms OR (newer.observed_at_ms=a.observed_at_ms AND newer.id>a.id)))
 ON CONFLICT(analysis_id) DO NOTHING`, s.Config.OwnerID)
	return err
}

func (s *Service) deliverAnalysisPost(ctx context.Context) (bool, error) {
	a := s.Archiver
	if a == nil || a.TG == nil || s.Config.OwnerID <= 0 || s.Config.OwnerID != a.Config.OwnerID || a.Config.BackupChatID == 0 || s.Config.ArchivePaused || a.Config.ArchivePaused {
		return false, nil
	}
	if err := s.Store.requireSyncPublication(ctx, s.Config.OwnerID); err != nil {
		if errors.Is(err, errSyncPublicationDeferred) {
			return false, nil
		}
		return false, err
	}
	sendCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	for !a.mu.TryLock() {
		if !sleep(sendCtx, 25*time.Millisecond) {
			return false, sendCtx.Err()
		}
	}
	defer a.mu.Unlock()

	unlock, locked, lockErr := s.Store.trySyncPublicationLock()
	if lockErr != nil {
		return false, lockErr
	}
	if !locked {
		return false, nil
	}
	defer unlock()
	if err := s.initAnalysisPosts(sendCtx); err != nil {
		return false, err
	}
	if err := a.init(sendCtx); err != nil {
		return false, err
	}
	if err := s.seedAnalysisPosts(sendCtx); err != nil {
		return false, err
	}
	presenceGuard, err := analysisPostPresenceGuard(sendCtx, s.Store)
	if err != nil {
		return false, err
	}
	tx, err := s.Store.DB.BeginTx(sendCtx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(sendCtx, "UPDATE meta SET value=value WHERE key='analysis_posts'"); err != nil {
		return false, err
	}
	var id, thread int64
	err = tx.QueryRowContext(sendCtx, `SELECT p.analysis_id,t.thread_id FROM analysis_posts p
 JOIN sessions se ON se.owner_id=p.owner_id AND se.upstream_id=p.upstream_id
 JOIN mirror_topics t ON t.profile_id=p.profile_id
 WHERE p.owner_id=? AND p.state='pending' AND p.next_attempt<=? AND t.thread_id>0 `+s.Store.syncPublicationCandidates("p.owner_id", "p.analysis_id", "analysis")+` AND `+s.Store.syncAnalysisPostReady()+presenceGuard+`
 ORDER BY p.analysis_id DESC LIMIT 1`, s.Config.OwnerID, a.now().Unix()).Scan(&id, &thread)
	if errors.Is(err, sql.ErrNoRows) {
		return false, tx.Commit()
	}
	if err != nil {
		return false, err
	}
	result, err := tx.ExecContext(sendCtx, "UPDATE analysis_posts SET state='preparing',chat_id=?,thread_id=?,error='',updated_at=CURRENT_TIMESTAMP WHERE analysis_id=? AND state='pending'", a.Config.BackupChatID, thread, id)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if n != 1 {
		return false, errors.New("analysis post claim lost")
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	analysis, err := scanProfileAnalysis(s.Store.DB.QueryRowContext(sendCtx, "SELECT "+analysisColumns+" FROM profile_analyses a WHERE a.id=? AND a.owner_id=?", id, s.Config.OwnerID))
	if err != nil {
		return true, s.finishAnalysisPost(ctx, id, "error", SentMessage{}, 0, "analysis_unavailable")
	}
	payload, err := analysisPostText(analysis)
	if err != nil {
		return true, s.finishAnalysisPost(ctx, id, "error", SentMessage{}, 0, "invalid_analysis_snapshot_or_response")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return true, err
	}
	if _, err = s.Store.DB.ExecContext(sendCtx, "UPDATE analysis_posts SET payload_json=?,response_sha=?,updated_at=CURRENT_TIMESTAMP WHERE analysis_id=? AND state='preparing'", raw, digest(analysis.ResponseBody), id); err != nil {
		return true, err
	}
	var message SentMessage
	var lastSendErr error
	attempted := false
	err = a.groupCall(sendCtx, func() error {
		if err := s.Store.requireSyncPublication(sendCtx, s.Config.OwnerID); err != nil {
			return err
		}
		// Repeat the binding check after a confirmed 429, without another state transition.
		binding := ` AND p.owner_id=? AND p.upstream_id=?
 AND EXISTS(SELECT 1 FROM sessions se WHERE se.owner_id=p.owner_id AND se.upstream_id=p.upstream_id)
 AND EXISTS(SELECT 1 FROM mirror_topics t WHERE t.profile_id=p.profile_id AND t.thread_id=p.thread_id)` + presenceGuard
		if !attempted {
			result, markErr := s.Store.DB.ExecContext(sendCtx, `UPDATE analysis_posts AS p SET state='sending',updated_at=CURRENT_TIMESTAMP WHERE analysis_id=? AND state='preparing'`+binding, id, s.Config.OwnerID, analysis.UpstreamID)
			if markErr != nil {
				return markErr
			}
			changed, markErr := result.RowsAffected()
			if markErr != nil {
				return markErr
			}
			if changed != 1 {
				return errors.New("analysis post source or topic changed")
			}
		} else {
			var allowed bool
			if markErr := s.Store.DB.QueryRowContext(sendCtx, `SELECT EXISTS(SELECT 1 FROM analysis_posts p WHERE analysis_id=? AND state='sending'`+binding+`)`, id, s.Config.OwnerID, analysis.UpstreamID).Scan(&allowed); markErr != nil {
				return markErr
			}
			if !allowed {
				return errors.New("analysis post source or topic changed")
			}
		}
		attempted = true
		message, lastSendErr = a.TG.SendText(sendCtx, a.Config.BackupChatID, thread, payload.Text, payload.Keyboard, payload.Entities)
		return lastSendErr
	})
	if err == nil && (message.ID <= 0 || message.Chat.ID != a.Config.BackupChatID || message.ThreadID != thread) {
		err = errors.New("analysis post receipt unconfirmed")
	}
	state, detail, next := "sent", "", int64(0)
	if err != nil {
		classification := err
		var lastRejected *TelegramError
		if errors.As(lastSendErr, &lastRejected) && lastRejected.Code == 429 {
			classification = lastSendErr
		}
		state, detail = "uncertain", archiveErrorCode(classification)
		var rejected *TelegramError
		if !attempted {
			state = "pending"
		} else if errors.As(classification, &rejected) {
			state = "error"
			if rejected.Code == 429 {
				state = "pending"
				next = a.now().Add(max(4*time.Second, time.Duration(rejected.RetryAfter)*time.Second)).Unix()
			}
		}
	}
	writeErr := s.finishAnalysisPost(ctx, id, state, message, next, detail)
	return true, errors.Join(err, writeErr)
}

func (s *Service) finishAnalysisPost(ctx context.Context, id int64, state string, message SentMessage, next int64, detail string) error {
	raw, err := json.Marshal(message)
	if err != nil {
		return err
	}
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	result, err := s.Store.DB.ExecContext(writeCtx, "UPDATE analysis_posts SET state=?,message_id=?,receipt_json=?,next_attempt=?,error=?,updated_at=CURRENT_TIMESTAMP WHERE analysis_id=? AND owner_id=? AND state IN ('preparing','sending')", state, message.ID, raw, next, detail, id, s.Config.OwnerID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("analysis post receipt not committed")
	}
	return nil
}

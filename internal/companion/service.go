package companion

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Service struct {
	Config   Config
	Store    *Store
	Client   *Client
	TG       *Telegram
	Media    *MediaManager
	Archiver *Archiver
	locks    sync.Map
	runs     sync.Map
}

func NewService(c Config) (*Service, error) {
	s, err := OpenStore(c)
	if err != nil {
		return nil, err
	}
	v := &Service{Config: c, Store: s, Client: NewClient(s, c), TG: NewTelegram(c), Media: NewMediaManager(s, c)}
	v.Archiver = NewArchiver(c, s, v.TG, v.Media)
	return v, nil
}
func (s *Service) Close() error { return s.Store.Close() }
func (s *Service) userLock(id int64) *sync.Mutex {
	v, _ := s.locks.LoadOrStore(id, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func (s *Service) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if s.Config.Token == "" {
		return errors.New("TELEGRAM_BOT_TOKEN is required")
	}
	var me TGUser
	if err := s.TG.Call(ctx, "getMe", map[string]any{}, &me); err != nil {
		return err
	}
	if !me.IsBot {
		return errors.New("configured Telegram identity is not a bot")
	}
	var wh struct {
		URL string `json:"url"`
	}
	if err := s.TG.Call(ctx, "getWebhookInfo", map[string]any{}, &wh); err != nil {
		return err
	}
	if wh.URL != "" {
		return errors.New("existing webhook configured; refusing simultaneous long polling")
	}
	commands := []map[string]string{{"command": "next", "description": "Следующая анкета"}, {"command": "status", "description": "Аккаунт и архив"}, {"command": "connect", "description": "Подключить свой Jardulik"}, {"command": "favorites", "description": "Избранные анкеты"}, {"command": "history", "description": "История анкет"}, {"command": "pause", "description": "Остановить действия"}, {"command": "disconnect", "description": "Отключить сессию Jardulik"}, {"command": "delete", "description": "Удалить свои данные"}, {"command": "help", "description": "Подключение и команды"}}
	if err := s.TG.Call(ctx, "setMyCommands", map[string]any{"commands": commands}, nil); err != nil {
		return err
	}
	var background sync.WaitGroup
	background.Add(2)
	go func() { defer background.Done(); s.Media.Run(ctx) }()
	go func() { defer background.Done(); s.Archiver.Run(ctx) }()
	queues := make([]chan Update, 4)
	var workers sync.WaitGroup
	for i := range queues {
		queues[i] = make(chan Update, 16)
		workers.Add(1)
		go func(jobs <-chan Update) {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case u, ok := <-jobs:
					if !ok {
						return
					}
					s.Handle(ctx, u)
					s.Store.FinishUpdate(ctx, u.ID)
				}
			}
		}(queues[i])
	}
	defer func() {
		cancel()
		for _, q := range queues {
			close(q)
		}
		workers.Wait()
		background.Wait()
	}()
	offset, _ := strconv.ParseInt(s.Store.Meta(ctx, "telegram_offset"), 10, 64)
	slog.Info("bot started", "username", me.Username, "version", Version, "reactions_enabled", s.Config.AllowReactions)
	for ctx.Err() == nil {
		var updates []Update
		err := s.TG.Call(ctx, "getUpdates", map[string]any{"offset": offset, "timeout": 30, "allowed_updates": []string{"message", "callback_query", "my_chat_member"}}, &updates)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			slog.Warn("Telegram polling unavailable", "kind", safeError(err))
			if !sleep(ctx, 5*time.Second) {
				break
			}
			continue
		}
		for _, u := range updates {
			claimed, err := s.Store.ClaimUpdate(ctx, u.ID)
			if err != nil {
				return err
			}
			if claimed {
				// Pause is not queued behind a long-running profile fetch.
				if u.Message != nil && strings.HasPrefix(u.Message.Text, "/pause") {
					s.Store.Pause(ctx, u.Message.From.ID, true)
					s.cancelRun(u.Message.From.ID)
				}
				select {
				case queues[updateShard(u, len(queues))] <- u:
				case <-ctx.Done():
					return nil
				}
			}
			offset = u.ID + 1
			if err = s.Store.SetMeta(ctx, "telegram_offset", strconv.FormatInt(offset, 10)); err != nil {
				return err
			}
		}
	}
	return nil
}

func safeError(err error) string {
	var te *TelegramError
	if errors.As(err, &te) {
		return fmt.Sprintf("telegram_%d", te.Code)
	}
	var ue *UpstreamError
	if errors.As(err, &ue) {
		return ue.Error()
	}
	return "operation_failed"
}
func updateShard(u Update, n int) int {
	var owner int64
	if u.Message != nil {
		owner = u.Message.From.ID
	} else if u.Callback != nil {
		owner = u.Callback.From.ID
	}
	if owner < 0 {
		owner = -owner
	}
	return int(uint64(owner) % uint64(n))
}
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
func (s *Service) say(ctx context.Context, id int64, text string, keyboard any) error {
	_, err := s.TG.SendText(ctx, id, 0, truncate(text, 4000), keyboard)
	return err
}
func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) > n {
		return string(r[:n-1]) + "…"
	}
	return s
}
func (s *Service) Handle(ctx context.Context, u Update) {
	var owner int64
	var m Message
	if u.Callback != nil {
		owner = u.Callback.From.ID
		m = u.Callback.Message
	} else if u.Message != nil {
		owner = u.Message.From.ID
		m = *u.Message
	} else {
		return
	}
	if m.Chat.Type != "private" || m.Chat.ID != owner {
		return
	}
	mu := s.userLock(owner)
	mu.Lock()
	defer mu.Unlock()
	if err := s.Store.User(ctx, owner, ""); err != nil {
		return
	}
	if u.Callback != nil {
		_ = s.TG.Call(ctx, "answerCallbackQuery", map[string]string{"callback_query_id": u.Callback.ID}, nil)
		parts := strings.Split(u.Callback.Data, ":")
		if len(parts) == 2 {
			id, _ := strconv.ParseInt(parts[1], 10, 64)
			switch parts[0] {
			case "f":
				if err := s.Store.Favorite(ctx, owner, id); err != nil {
					_ = s.say(ctx, owner, "Анкета недоступна.", nil)
				} else {
					_ = s.say(ctx, owner, "Сохранено в избранное.", nil)
				}
			case "h":
				s.history(ctx, owner, id)
			case "p":
				if p, err := s.Store.Profile(ctx, owner, id); err == nil {
					_ = s.Deliver(ctx, owner, p)
				}
			}
		} else if u.Callback.Data == "next" {
			s.next(ctx, owner)
		}
		return
	}
	fields := strings.Fields(m.Text)
	if len(fields) == 0 {
		return
	}
	cmd := strings.Split(fields[0], "@")[0]
	switch cmd {
	case "/start":
		source := "direct"
		if len(fields) > 1 && validSource(fields[1]) {
			source = fields[1]
		}
		_, _ = s.Store.DB.ExecContext(ctx, "UPDATE users SET source=CASE WHEN source='' THEN ? ELSE source END WHERE id=?", source, owner)
		s.Store.Event(ctx, owner, "start")
		_ = s.say(ctx, owner, "Jardulik Companion\n\nАнкеты, фотографии, избранное и история в этом чате. Это независимый помощник.\n\nДля первого подключения нужен URL Jardulik из Telegram Web. Пароль, коды входа и Telegram-сессия не нужны. /connect покажет инструкцию.\n\nСохраняем все анкеты и фотографии, которые Jardulik вернул твоему аккаунту. Оператор имеет доступ к архиву для работы сервиса и модерации; бэкап находится в закрытой Telegram-группе. Другие пользователи твои действия и заметки не видят.\n\nАвтолайки сейчас выключены. /disconnect останавливает доступ к Jardulik, /delete удаляет твои данные из рабочей базы.\n\nПолитика: https://kostia7alania.github.io/jardulik-companion-site/privacy.html", nil)
	case "/help":
		_ = s.say(ctx, owner, connectHelp+"\n/next /status /favorites /history /pause /disconnect /delete\nСвязаться с оператором: /support текст", nil)
	case "/support":
		text := strings.TrimSpace(strings.TrimPrefix(m.Text, fields[0]))
		if text == "" {
			_ = s.say(ctx, owner, "Отправь /support и текст обращения. Его получит оператор сервиса.", nil)
			return
		}
		if owner != s.Config.OwnerID {
			_ = s.say(ctx, s.Config.OwnerID, fmt.Sprintf("Обращение от Telegram ID %d:\n%s", owner, truncate(text, 2500)), nil)
		}
		_ = s.say(ctx, owner, "Обращение передано оператору.", nil)
	case "/connect":
		arg := strings.TrimSpace(strings.TrimPrefix(m.Text, fields[0]))
		if arg == "" {
			_ = s.say(ctx, owner, connectHelp, nil)
			return
		}
		// The launch URL contains a credential. Delete the submitted private message without persisting its body.
		defer s.TG.Call(context.WithoutCancel(ctx), "deleteMessage", map[string]any{"chat_id": owner, "message_id": m.ID}, nil)
		raw, err := ParseInitData(arg)
		if err == nil {
			err = s.Media.CheckDisk()
		}
		if err == nil {
			err = s.Client.Login(ctx, owner, raw)
		}
		if err != nil {
			_ = s.say(ctx, owner, "Не удалось подключить: "+err.Error(), nil)
			return
		}
		account, err := s.Client.Me(ctx, owner)
		if err != nil {
			_ = s.say(ctx, owner, "Сессия получена, но проверка аккаунта не завершилась. /status", nil)
			return
		}
		_ = s.say(ctx, owner, "Аккаунт подключён. "+accountSummary(account)+"\n/next покажет первую анкету. Лайки выключены.", nil)
	case "/next":
		s.next(ctx, owner)
	case "/status":
		s.status(ctx, owner)
	case "/favorites":
		s.list(ctx, owner, true)
	case "/history":
		if len(fields) > 1 {
			id, _ := strconv.ParseInt(fields[1], 10, 64)
			s.history(ctx, owner, id)
		} else {
			s.list(ctx, owner, false)
		}
	case "/pause":
		s.Store.Pause(ctx, owner, true)
		s.cancelRun(owner)
		_ = s.say(ctx, owner, "Действия остановлены. Архив сохранён. /next остаётся ручной командой.", nil)
	case "/resume":
		s.Store.Pause(ctx, owner, false)
		_ = s.say(ctx, owner, "Ручной режим доступен. Это не запускает автолайки.", nil)
	case "/disconnect":
		s.cancelRun(owner)
		err := s.Store.Disconnect(ctx, owner)
		if err == nil {
			_ = s.say(ctx, owner, "Сессия удалена. Новые запросы к Jardulik остановлены. Архив доступен через /history; полное удаление: /delete.", nil)
		}
	case "/delete":
		if len(fields) != 2 || fields[1] != "CONFIRM" {
			_ = s.say(ctx, owner, "Удалить твою сессию, историю, избранное и настройки из рабочей базы? Это необратимо. Для подтверждения отправь /delete CONFIRM.\nСуществующие Telegram-бэкапы и модерационные копии требуют отдельного удаления оператором. Файлы, доступные другим аккаунтам, могут сохраниться у них.", nil)
			return
		}
		s.cancelRun(owner)
		err := s.Store.DeleteUser(ctx, owner)
		if err == nil {
			_ = s.say(ctx, owner, "Твои данные удалены из рабочей базы. Для удаления архивных Telegram-копий обратись к оператору через /help.", nil)
			if owner != s.Config.OwnerID {
				_ = s.say(ctx, s.Config.OwnerID, fmt.Sprintf("Запрос удаления: Telegram ID %d. Рабочая база очищена, нужна обработка архивных копий и deletion log перед восстановлением старых бэкапов.", owner), nil)
			}
		}
	case "/autolikes":
		if !s.Config.AllowReactions {
			_ = s.say(ctx, owner, "Автолайки выключены администратором. Никаких реакций не отправлено.", nil)
			return
		}
		n := int64(0)
		if len(fields) == 2 {
			n, _ = strconv.ParseInt(fields[1], 10, 64)
		}
		if n < 1 || n > 50 {
			_ = s.say(ctx, owner, "Нужен явный лимит серии: /autolikes N, от 1 до 50.", nil)
			return
		}
		s.startRun(ctx, owner, int(n))
	case "/stats":
		if owner == s.Config.OwnerID {
			b, _ := json.Marshal(s.Store.AdminCounts(ctx))
			_ = s.say(ctx, owner, "Фактические события бота: "+string(b)+"\nЭто не посещения сайта и не прочтения сообщений.", nil)
		}
	case "/backup":
		if owner == s.Config.OwnerID {
			err := s.Archiver.RequestBackup(ctx)
			if err != nil {
				_ = s.say(ctx, owner, "Не удалось поставить бэкап в очередь.", nil)
			} else {
				_ = s.say(ctx, owner, "Бэкап поставлен в очередь. "+s.Archiver.BackupStatus(ctx), nil)
			}
		}
	case "/register_source":
		if owner != s.Config.OwnerID {
			return
		}
		if m.ForwardOrigin == nil || m.ForwardOrigin.Type != "user" || m.ForwardOrigin.Sender.Username != "jardulik_bot" || !m.ForwardOrigin.Sender.IsBot {
			_ = s.say(ctx, owner, "Перешли сообщение официального @jardulik_bot, чтобы закрепить источник.", nil)
			return
		}
		_ = s.Store.SetMeta(ctx, "source_bot_id", strconv.FormatInt(m.ForwardOrigin.Sender.ID, 10))
		_ = s.say(ctx, owner, "Источник зарегистрирован. Конфигурация должна совпадать с этим ID.", nil)
	default:
		_ = s.say(ctx, owner, connectHelp+"\nКоманды: /next /status /favorites /history /pause /disconnect /delete\nОператору: отправь сообщение командой /support текст.", nil)
	}
}

const connectHelp = "Подключение Jardulik\n\n1. Открой официальный @jardulik_bot в Telegram Web на компьютере и запусти приложение.\n2. В инструментах разработчика браузера найди iframe с адресом mini.jardulik.app и скопируй полный src, содержащий #tgWebAppData=. Это временный ключ только к Jardulik.\n3. Отправь сюда /connect и ссылку одним сообщением. После обработки сообщение со ссылкой удаляется.\n\nНе присылай пароль, код Telegram, bot token или файлы Telegram-сессии. Ссылка должна принадлежать тому же аккаунту, с которого ты пишешь. Если нет полной ссылки, пока используй официальное приложение."

func validSource(v string) bool {
	if len(v) > 64 {
		return false
	}
	for _, r := range v {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}
func accountSummary(p Profile) string {
	tier := "FREE"
	if number(p["type"]) == 1 {
		tier = "PREMIUM"
	}
	end := p.String("orderEndDate")
	if end != "" {
		if t, err := time.Parse(time.RFC3339, end); err == nil {
			end = t.In(time.FixedZone("МСК", 3*3600)).Format("02.01.2006 15:04 МСК")
		}
	}
	result := fmt.Sprintf("Jardulik ID %d, %s", p.ID(), tier)
	if end != "" {
		result += " до " + end
	}
	return result
}
func (s *Service) status(ctx context.Context, owner int64) {
	p, err := s.Client.Me(ctx, owner)
	state := "Jardulik не подключён"
	if err == nil {
		state = accountSummary(p)
	} else if session, e := s.Store.Session(ctx, owner); e == nil {
		state = "Состояние сессии: " + session.State
	}
	n := s.Store.Counts(ctx, owner)
	text := fmt.Sprintf("%s\nАнкет: %d, версий: %d\nФото сохранено: %d, в очереди: %d\nИзбранное: %d\nЛайков подтверждено: %d\nАвтолайки: %t", state, n["profiles"], n["versions"], n["photos"], n["pending_photos"], n["favorites"], n["likes"], s.Config.AllowReactions)
	if owner == s.Config.OwnerID {
		text += "\nБэкап: " + s.Archiver.BackupStatus(ctx)
	}
	_ = s.say(ctx, owner, text, nil)
}
func (s *Service) Fetch(ctx context.Context, owner int64) (int, error) {
	if err := s.Media.CheckDisk(); err != nil {
		return 0, err
	}
	if _, err := s.Client.Me(ctx, owner); err != nil {
		return 0, err
	}
	profiles, err := s.Client.Users(ctx, owner, s.Store.Page(ctx, owner))
	if err != nil {
		return 0, err
	}
	if _, err = s.Store.Archive(ctx, owner, profiles, "GET /user/getUsers"); err != nil {
		return 0, err
	}
	s.Store.AdvancePage(ctx, owner, len(profiles) == 0)
	return len(profiles), nil
}
func (s *Service) next(ctx context.Context, owner int64) {
	p, err := s.Store.Next(ctx, owner)
	if errors.Is(err, sql.ErrNoRows) {
		n, e := s.Fetch(ctx, owner)
		if e != nil {
			_ = s.say(ctx, owner, "Не удалось получить выдачу: "+e.Error(), nil)
			return
		}
		if n == 0 {
			_ = s.say(ctx, owner, "Jardulik не вернул новые анкеты по этим условиям. Фильтры автоматически не расширяем.", nil)
			return
		}
		p, err = s.Store.Next(ctx, owner)
	}
	if err != nil {
		_ = s.say(ctx, owner, "В этой пачке нет ещё не показанных анкет. Они сохранены в архиве; попробуй /next позже.", nil)
		return
	}
	if full, e := s.Client.FullProfile(ctx, owner, p.ID); e == nil {
		if _, e = s.Store.Archive(ctx, owner, []Profile{full}, "GET /user/getUser"); e == nil {
			p, _ = s.Store.Profile(ctx, owner, p.ID)
		}
	}
	if err = s.Deliver(ctx, owner, p); err != nil {
		_ = s.say(ctx, owner, "Выдача карточки прервалась. Архив сохранён. Проверь чат перед повторной командой.", nil)
		slog.Warn("profile delivery incomplete", "tenant", owner, "kind", safeError(err))
	}
}
func profileText(p StoredProfile) string {
	v := p.Profile
	var b strings.Builder
	fmt.Fprintf(&b, "%s", v.String("name"))
	if age := v.String("age"); age != "" {
		fmt.Fprintf(&b, ", %s", age)
	}
	fmt.Fprintf(&b, "\nJardulik #%d\n", p.ID)
	for _, f := range []struct{ Key, Label string }{{"surName", "Фамилия"}, {"country", "Страна"}, {"city", "Город"}, {"height", "Рост"}, {"goal", "Цель"}, {"worldview", "Мировоззрение"}, {"children", "Дети"}, {"profession", "Занятие"}, {"familyNames", "Родственные фамилии"}, {"languages", "Языки"}, {"description", "О себе"}} {
		if val := v.String(f.Key); val != "" && val != "null" {
			fmt.Fprintf(&b, "\n%s: %s", f.Label, val)
		}
	}
	fmt.Fprintf(&b, "\n\nСнимок: %s UTC\nИстория: /history %d", p.LastSeen, p.ID)
	return b.String()
}
func (s *Service) Deliver(ctx context.Context, owner int64, p StoredProfile) error {
	if _, err := s.Store.Version(ctx, owner, p.VersionID); err != nil {
		return err
	}
	_ = s.Media.EnsureProfile(ctx, owner, p.VersionID)
	r, err := s.Store.DB.ExecContext(ctx, "INSERT INTO deliveries(owner_id,version_id,state) VALUES(?,?,'pending')", owner, p.VersionID)
	if err != nil {
		return err
	}
	deliveryID, _ := r.LastInsertId()
	var ids []int64
	state := "unknown"
	defer func() {
		b, _ := json.Marshal(ids)
		_, _ = s.Store.DB.ExecContext(context.WithoutCancel(ctx), "UPDATE deliveries SET state=?,message_ids=? WHERE id=?", state, string(b), deliveryID)
	}()
	jobs, err := s.Store.MediaJobs(ctx, owner, p.VersionID)
	if err != nil {
		return err
	}
	hasFull := false
	for _, j := range jobs {
		if j.Role == "photos" && j.State == "complete" {
			hasFull = true
		}
	}
	for _, j := range jobs {
		if j.State != "complete" || (hasFull && j.Role != "photos") {
			continue
		}
		var msg SentMessage
		var err error
		if j.Size > 9<<20 {
			msg, err = s.TG.SendDocument(ctx, owner, 0, j.Path, fmt.Sprintf("Фото анкеты #%d", p.ID))
		} else {
			msg, err = s.TG.SendPhoto(ctx, owner, 0, j.Path, "")
			var rejected *TelegramError
			if errors.As(err, &rejected) && rejected.Code == 400 {
				msg, err = s.TG.SendDocument(ctx, owner, 0, j.Path, fmt.Sprintf("Исходное фото анкеты #%d", p.ID))
			}
		}
		if err != nil {
			return err
		}
		ids = append(ids, msg.ID)
		if !sleep(ctx, 1100*time.Millisecond) {
			return ctx.Err()
		}
	}
	buttons := map[string]any{"inline_keyboard": [][]map[string]string{{{"text": "В избранное", "callback_data": fmt.Sprintf("f:%d", p.ID)}, {"text": "История", "callback_data": fmt.Sprintf("h:%d", p.ID)}}, {{"text": "Следующая анкета", "callback_data": "next"}}}}
	text := profileText(p)
	runes := []rune(text)
	for len(runes) > 0 {
		n := min(len(runes), 3500)
		var keyboard any
		if n == len(runes) {
			keyboard = buttons
		}
		msg, err := s.TG.SendText(ctx, owner, 0, string(runes[:n]), keyboard)
		if err != nil {
			return err
		}
		ids = append(ids, msg.ID)
		runes = runes[n:]
		if len(runes) > 0 && !sleep(ctx, time.Second) {
			return ctx.Err()
		}
	}
	state = "sent"
	s.Store.Delivered(ctx, owner, p.ID)
	s.Store.Event(ctx, owner, "card_delivered")
	return nil
}
func (s *Service) list(ctx context.Context, owner int64, favorites bool) {
	a, err := s.Store.List(ctx, owner, favorites)
	if err != nil {
		return
	}
	if len(a) == 0 {
		_ = s.say(ctx, owner, "Пока пусто. /next покажет анкеты.", nil)
		return
	}
	var b strings.Builder
	for _, p := range a {
		fmt.Fprintf(&b, "%s, %s · #%d\n/history %d\n\n", p.Profile.String("name"), p.Profile.String("age"), p.ID, p.ID)
	}
	_ = s.say(ctx, owner, b.String(), nil)
}
func (s *Service) history(ctx context.Context, owner, id int64) {
	a, err := s.Store.History(ctx, owner, id)
	if err != nil || len(a) == 0 {
		_ = s.say(ctx, owner, "В твоём архиве такой анкеты нет.", nil)
		return
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Анкета #%d: %s\nВерсий в этой выборке: %d\n", id, a[0].Profile.String("name"), len(a))
	for i, p := range a {
		fmt.Fprintf(&b, "\n%s UTC", p.LastSeen)
		if i+1 < len(a) {
			var changed []string
			for _, key := range []string{"name", "age", "city", "description", "familyNames", "height", "goal", "worldview", "profession", "children", "photos", "miniPhotos"} {
				if p.Profile.String(key) != a[i+1].Profile.String(key) {
					changed = append(changed, key)
				}
			}
			if len(changed) > 0 {
				b.WriteString(" · изменилось: " + strings.Join(changed, ", "))
			}
		}
	}
	_ = s.say(ctx, owner, b.String(), map[string]any{"inline_keyboard": [][]map[string]string{{{"text": "Показать последнюю версию", "callback_data": fmt.Sprintf("p:%d", id)}}}})
}
func (s *Service) cancelRun(owner int64) {
	if v, ok := s.runs.Load(owner); ok {
		v.(context.CancelFunc)()
	}
}
func (s *Service) startRun(parent context.Context, owner int64, count int) {
	ctx, cancel := context.WithCancel(parent)
	if _, loaded := s.runs.LoadOrStore(owner, context.CancelFunc(cancel)); loaded {
		cancel()
		_ = s.say(parent, owner, "Серия уже выполняется. /pause остановит её.", nil)
		return
	}
	go func() {
		defer s.runs.Delete(owner)
		defer cancel()
		done := 0
		for done < count && ctx.Err() == nil && !s.Store.Paused(ctx, owner) {
			p, err := s.Store.Next(ctx, owner)
			if errors.Is(err, sql.ErrNoRows) {
				_, err = s.Fetch(ctx, owner)
				if err == nil {
					p, err = s.Store.Next(ctx, owner)
				}
			}
			if err != nil {
				break
			}
			account, err := s.Client.Me(ctx, owner)
			if err != nil || account["freezeState"] == true {
				break
			}
			result, err := s.Client.Like(ctx, owner, p.ID)
			if err != nil || !result.Success {
				break
			}
			done++
			s.Store.Delivered(ctx, owner, p.ID)
			if !sleep(ctx, 10*time.Second) {
				break
			}
		}
		_ = s.say(context.WithoutCancel(parent), owner, fmt.Sprintf("Серия завершена. Подтверждено лайков: %d из %d.", done, count), nil)
	}()
	_ = s.say(parent, owner, fmt.Sprintf("Запущена серия до %d лайков по текущей выдаче. /pause остановит следующие действия.", count), nil)
}

// WriteRecoveryKey keeps the off-server restore key out of Git and service archives.
func WriteRecoveryKey(c Config, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	return os.WriteFile(path, []byte("Jardulik Companion recovery key\nBACKUP_KEY="+base64.StdEncoding.EncodeToString(c.BackupKey)+"\nСохрани этот ключ отдельно от архивной группы. Без него сервисный архив нельзя расшифровать.\n"), 0600)
}

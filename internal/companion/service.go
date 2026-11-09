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
	Config     Config
	Store      *Store
	Client     *Client
	TG         *Telegram
	Media      *MediaManager
	Archiver   *Archiver
	Watcher    *ProfileWatcher
	Crawler    *FeedCrawler
	Autoliker  *Autoliker
	foreground foregroundPriority
	locks      sync.Map
	runs       sync.Map
}

func NewService(c Config) (*Service, error) {
	s, err := OpenStore(c)
	if err != nil {
		return nil, err
	}
	v := &Service{Config: c, Store: s, Client: NewClient(s, c), TG: NewTelegram(c), Media: NewMediaManager(s, c)}
	v.Archiver = NewArchiver(c, s, v.TG, v.Media)
	v.Watcher = NewProfileWatcher(c, s, v.Client, v.Archiver)
	v.Crawler = NewFeedCrawler(v)
	v.Autoliker = NewAutoliker(v)
	return v, nil
}
func (s *Service) Close() error { return s.Store.Close() }
func (s *Service) userLock(id int64) *sync.Mutex {
	v, _ := s.locks.LoadOrStore(id, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func (s *Service) Run(ctx context.Context) error {
	return s.runBot(ctx, RuntimeAll)
}

func (s *Service) RunBotParser(ctx context.Context) error {
	return s.runBot(ctx, RuntimeBotParser)
}

func (s *Service) runBot(ctx context.Context, role RuntimeRole) error {
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
	commands := []map[string]string{{"command": "next", "description": "Следующая анкета"}, {"command": "status", "description": "Аккаунт и архив"}, {"command": "connect", "description": "Подключить свой Jardulik"}, {"command": "favorites", "description": "Избранные анкеты"}, {"command": "likes", "description": "Входящие лайки"}, {"command": "matches", "description": "Взаимности"}, {"command": "filters", "description": "Фильтры Jardulik"}, {"command": "history", "description": "История анкет"}, {"command": "pause", "description": "Остановить действия"}, {"command": "disconnect", "description": "Отключить сессию Jardulik"}, {"command": "delete", "description": "Удалить свои данные"}, {"command": "help", "description": "Подключение и команды"}}
	commands = append(commands, map[string]string{"command": "settings", "description": "Настройки и автолайки"}, map[string]string{"command": "autolike", "description": "Управление автолайками"})
	if err := s.TG.Call(ctx, "setMyCommands", map[string]any{"commands": commands}, nil); err != nil {
		return err
	}
	waitBackground, err := s.RunBackground(ctx, role)
	if err != nil {
		return err
	}
	if err := s.bootstrapKeyboards(ctx); err != nil {
		slog.Warn("keyboard installation incomplete", "kind", safeError(err))
	}
	queues := make([]chan queuedUpdate, 4)
	var workers sync.WaitGroup
	for i := range queues {
		queues[i] = make(chan queuedUpdate, 16)
		workers.Add(1)
		go func(jobs <-chan queuedUpdate) {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case u, ok := <-jobs:
					if !ok {
						return
					}
					s.handleQueuedUpdate(ctx, u)
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
		for _, q := range queues {
			s.discardQueuedUpdates(q)
		}
		waitBackground()
	}()
	offset, _ := strconv.ParseInt(s.Store.Meta(ctx, "telegram_offset"), 10, 64)
	slog.Info("bot started", "username", me.Username, "version", Version, "reactions_enabled", s.Config.AllowReactions)
	for ctx.Err() == nil {
		var updates []Update
		err := s.TG.Call(ctx, "getUpdates", map[string]any{"offset": offset, "timeout": 30, "allowed_updates": []string{"message", "callback_query", "my_chat_member", "business_connection", "business_message", "edited_business_message"}}, &updates)
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
			// Commit contact receipts before acknowledging the Telegram offset.
			// Serial ingestion also preserves connection/revocation ordering.
			if handled, err := s.Store.IngestBusinessContactUpdate(ctx, u); err != nil {
				return err
			} else if handled {
				offset = u.ID + 1
				if err = s.Store.SetMeta(ctx, "telegram_offset", strconv.FormatInt(offset, 10)); err != nil {
					return err
				}
				continue
			}
			claimed, err := s.Store.ClaimUpdate(ctx, u.ID)
			if err != nil {
				return err
			}
			if claimed {
				job := s.prepareQueuedUpdate(u)
				// Pause is not queued behind a long-running profile fetch.
				if isPrivatePause(u.Message) {
					s.Store.Pause(ctx, u.Message.From.ID, true)
					_ = s.Store.StopCrawl(ctx, u.Message.From.ID, "paused")
					s.cancelRun(u.Message.From.ID)
				}
				if u.Callback != nil && u.Callback.Message.Chat.Type == "private" && u.Callback.Message.Chat.ID == u.Callback.From.ID && strings.HasPrefix(u.Callback.Data, "as:") && strings.HasSuffix(u.Callback.Data, ":off") {
					_ = s.DisableAutolike(ctx, u.Callback.From.ID, "user_off")
				}
				if isPrivateCommand(u.Message, "/autolike_off") {
					_ = s.DisableAutolike(ctx, u.Message.From.ID, "user_off")
				}
				if !s.enqueueUpdate(ctx, queues[updateShard(u, len(queues))], job) {
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
func isPrivatePause(m *Message) bool {
	return isPrivateCommand(m, "/pause")
}
func isPrivateCommand(m *Message, command string) bool {
	if m == nil || m.Chat.Type != "private" || m.Chat.ID != m.From.ID {
		return false
	}
	fields := strings.Fields(m.Text)
	return len(fields) > 0 && strings.Split(fields[0], "@")[0] == command
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
	foreground := s.isForegroundUpdate(u)
	if foreground {
		s.beginForeground()
		defer s.endForeground()
	}
	s.handle(ctx, u)
}

func (s *Service) handle(ctx context.Context, u Update) {
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
	if u.Callback != nil && owner == s.Config.OwnerID && m.Chat.ID == s.Config.BackupChatID && s.Config.BackupChatID != 0 {
		if strings.HasPrefix(u.Callback.Data, "av:") || strings.HasPrefix(u.Callback.Data, "az:") || strings.HasPrefix(u.Callback.Data, "aj:") {
			s.archiveCallback(ctx, *u.Callback)
		}
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
		if handled, err := s.HandleSettingsMessageCallback(ctx, owner, m.ID, u.Callback.Data); handled {
			if err != nil {
				_ = s.say(ctx, owner, "Не удалось изменить настройки: "+err.Error(), nil)
			}
			return
		}
		if s.FilterMessageCallback(ctx, owner, m.ID, u.Callback.Data) {
			return
		}
		if s.HandleRelationCallback(ctx, owner, u.Callback.Data) {
			return
		}
		parts := strings.Split(u.Callback.Data, ":")
		if len(parts) == 2 {
			id, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil || id < 0 {
				return
			}
			if parts[0] == "lh" || parts[0] == "lf" {
				s.list(ctx, owner, parts[0] == "lf", id)
				return
			}
			if parts[0] == "ai" {
				if id <= int64(^uint(0)>>1) {
					s.showAgeIndex(ctx, owner, int(id))
				}
				return
			}
			if id == 0 {
				return
			}
			switch parts[0] {
			case "l":
				s.manualLike(ctx, owner, id)
			case "jv":
				s.sourceJSON(ctx, owner, owner, id)
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
			case "t":
				if p, err := s.Store.Profile(ctx, owner, id); err == nil {
					s.fullText(ctx, owner, p)
				}
			}
		} else {
			switch u.Callback.Data {
			case "next":
				s.next(ctx, owner)
			case "favorites":
				s.list(ctx, owner, true, 0)
			case "history":
				s.list(ctx, owner, false, 0)
			case "status":
				s.status(ctx, owner)
			case "filters":
				_ = s.OpenFiltersMessage(ctx, owner, m.ID)
			case "settings":
				_ = s.OpenSettingsMessage(ctx, owner, m.ID)
			case "likes":
				s.OpenRelations(ctx, owner, false)
			case "matches":
				s.OpenRelations(ctx, owner, true)
			case "connect_help":
				_ = s.say(ctx, owner, connectHelp, mainButtons())
			case "help":
				_ = s.say(ctx, owner, shortHelp, mainButtons())
			}
		}
		return
	}
	if s.handleReplyKeyboardLocked(ctx, owner, m) {
		return
	}
	fields := strings.Fields(m.Text)
	if len(fields) == 0 {
		return
	}
	if !strings.HasPrefix(fields[0], "/") && s.FilterText(ctx, owner, m.Text) {
		return
	}
	cmd := strings.Split(fields[0], "@")[0]
	for _, prefix := range []string{"/text_", "/fav_", "/history_", "/json_", "/like_"} {
		if !strings.HasPrefix(cmd, prefix) {
			continue
		}
		id, err := strconv.ParseInt(strings.TrimPrefix(cmd, prefix), 10, 64)
		if err != nil || id <= 0 {
			return
		}
		if prefix == "/like_" {
			s.manualLike(ctx, owner, id)
			return
		}
		if prefix == "/json_" {
			s.sourceJSON(ctx, owner, owner, id)
			return
		}
		if prefix == "/history_" {
			s.history(ctx, owner, id)
			return
		}
		if p, err := s.Store.Profile(ctx, owner, id); err == nil {
			if prefix == "/text_" {
				s.fullText(ctx, owner, p)
			} else if s.Store.Favorite(ctx, owner, id) == nil {
				_ = s.say(ctx, owner, "Сохранено в избранное.", nil)
			}
		}
		return
	}
	if handled, err := s.HandleAutolikeCommand(ctx, owner, m.Text); handled {
		if err != nil {
			_ = s.say(ctx, owner, "Не удалось изменить настройки: "+err.Error(), nil)
		}
		return
	}
	switch cmd {
	case "/start":
		source := "direct"
		if len(fields) > 1 && validSource(fields[1]) {
			source = fields[1]
		}
		_, _ = s.Store.DB.ExecContext(ctx, "UPDATE users SET source=CASE WHEN source='' THEN ? ELSE source END WHERE id=?", source, owner)
		s.Store.Event(ctx, owner, "start")
		_ = s.say(ctx, owner, welcomeText, keyboardReplyMarkup())
		s.showKeyboardMenuLocked(ctx, owner)
	case "/ageindex":
		s.showAgeIndex(ctx, owner, 0)
	case "/help":
		_ = s.say(ctx, owner, shortHelp, mainButtons())
	case "/support":
		text := strings.TrimSpace(strings.TrimPrefix(m.Text, fields[0]))
		if text == "" {
			_ = s.say(ctx, owner, "Отправь /support и текст обращения. Его получит оператор сервиса.", nil)
			return
		}
		if owner == s.Config.OwnerID {
			_ = s.say(ctx, owner, "Сообщение уже в чате оператора.", nil)
			return
		}
		excerpt, rest := telegramPrefix(text, 2500)
		if rest != "" {
			excerpt += "…"
		}
		sent, err := s.TG.SendText(ctx, s.Config.OwnerID, 0, fmt.Sprintf("Обращение от Telegram ID %d:\n%s", owner, excerpt), nil)
		if err != nil || sent.ID <= 0 || sent.Chat.ID != s.Config.OwnerID {
			_ = s.say(ctx, owner, "Доставка обращения не подтверждена. Попробуй позже.", nil)
			return
		}
		_ = s.say(ctx, owner, "Обращение передано оператору.", nil)
	case "/connect":
		arg := strings.TrimSpace(strings.TrimPrefix(m.Text, fields[0]))
		if arg == "" {
			_ = s.say(ctx, owner, connectHelp, mainButtons())
			return
		}
		// The launch URL contains a credential. Delete the submitted private message without persisting its body.
		defer s.TG.Call(context.WithoutCancel(ctx), "deleteMessage", map[string]any{"chat_id": owner, "message_id": m.ID}, nil)
		raw, err := ParseInitData(arg)
		if err == nil {
			err = s.Media.CheckDisk()
		}
		var account Profile
		if err == nil {
			account, err = s.Client.LoginAccount(ctx, owner, raw)
		}
		if err != nil {
			_ = s.say(ctx, owner, "Не удалось подключить: "+err.Error(), mainButtons())
			return
		}
		_ = s.say(ctx, owner, "✅ Jardulik подключён. "+accountSummary(account), map[string]any{"inline_keyboard": [][]map[string]string{
			{{"text": "▶ Автолайкер", "callback_data": "settings"}},
			{{"text": "Следующая анкета", "callback_data": "next"}},
		}})
	case "/next":
		s.next(ctx, owner)
	case "/status":
		s.status(ctx, owner)
	case "/filters":
		_ = s.OpenFilters(ctx, owner)
	case "/favorites":
		s.list(ctx, owner, true, 0)
	case "/likes":
		s.OpenRelations(ctx, owner, false)
	case "/matches":
		s.OpenRelations(ctx, owner, true)
	case "/history":
		if len(fields) > 1 {
			id, _ := strconv.ParseInt(fields[1], 10, 64)
			s.history(ctx, owner, id)
		} else {
			s.list(ctx, owner, false, 0)
		}
	case "/text":
		if len(fields) > 1 {
			id, _ := strconv.ParseInt(fields[1], 10, 64)
			if p, err := s.Store.Profile(ctx, owner, id); err == nil {
				s.fullText(ctx, owner, p)
			}
		}
	case "/pause":
		s.Store.Pause(ctx, owner, true)
		_ = s.Store.StopCrawl(ctx, owner, "paused")
		s.cancelRun(owner)
		_ = s.say(ctx, owner, "Действия остановлены. Архив сохранён. /next остаётся ручной командой.", nil)
	case "/resume":
		s.Store.Pause(ctx, owner, false)
		_ = s.say(ctx, owner, "Ручной режим доступен. Это не запускает автолайки.", nil)
	case "/disconnect":
		s.cancelRun(owner)
		_ = s.Store.StopCrawl(ctx, owner, "disconnected")
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
				_ = s.say(ctx, owner, "Полный бэкап поставлен в очередь. "+s.Archiver.BackupStatus(ctx), nil)
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
		_ = s.say(ctx, owner, shortHelp, mainButtons())
	}
}

const welcomeText = "Jardulik Companion\nНезависимый помощник: анкеты, фото, избранное и история. Выбери действие ниже.\n\nСохраняем полученные анкеты и фото. Оператор имеет доступ к архиву для работы сервиса и модерации; копия хранится в закрытой Telegram-группе. Другие пользователи твою историю не видят.\n\nПолитика: https://kostia7alania.ru/jardulik-companion-site/privacy.html"

const shortHelp = "Выбери действие кнопкой. Для подключения: /connect\n\n❤️ Лайк отправляет реакцию в Jardulik. Избранное хранится только в боте.\n/settings открывает настройки и управление автолайками.\n/pause останавливает фоновые действия.\n/disconnect отключает Jardulik, архив остаётся.\n/delete удаляет данные из рабочей базы.\nОператору: /support текст"

const connectHelp = "Подключение Jardulik\n\n1. Открой официальный @jardulik_bot в Telegram Web на компьютере и запусти приложение.\n2. В инструментах разработчика браузера найди iframe с адресом mini.jardulik.app и скопируй полный src, содержащий #tgWebAppData=.\n3. Отправь сюда /connect и ссылку одним сообщением. После обработки бот попробует удалить сообщение со ссылкой."

func mainButtons() any {
	return map[string]any{"inline_keyboard": [][]map[string]string{
		{{"text": "Следующая анкета", "callback_data": "next"}, {"text": "Избранное", "callback_data": "favorites"}},
		{{"text": "Входящие лайки", "callback_data": "likes"}, {"text": "Взаимности", "callback_data": "matches"}},
		{{"text": "История", "callback_data": "history"}, {"text": "Аккаунт", "callback_data": "status"}},
		{{"text": "Фильтры", "callback_data": "filters"}, {"text": "Настройки", "callback_data": "settings"}},
		{{"text": "Подключение", "callback_data": "connect_help"}, {"text": "Помощь", "callback_data": "help"}},
		{jardulikAppButton()},
	}}
}

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
		end = displayTime(end)
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
		for _, field := range []struct{ key, label string }{{"fire", "🔥 Огонь"}, {"clean", "Чистый лист"}} {
			if value, present := p[field.key]; present && value != nil {
				if n, e := strconv.ParseInt(valueString(value), 10, 64); e == nil && n >= 0 {
					state += fmt.Sprintf("\n%s: %d", field.label, n)
				}
			}
		}
	} else if session, e := s.Store.Session(ctx, owner); e == nil {
		state = "Состояние сессии: " + session.State
	}
	n := s.Store.Counts(ctx, owner)
	mode := "выключены"
	if s.Config.AllowReactions {
		mode = "включены"
	}
	text := fmt.Sprintf("%s\nАнкет: %d, снимков: %d\nФото сохранено: %d, в очереди: %d\nИзбранное: %d\nОтправлено лайков ботом: %d\nРеакции в Jardulik: %s", state, n["profiles"], n["versions"], n["photos"], n["pending_photos"], n["favorites"], n["likes"], mode)
	text += "\n" + s.AutolikeStatus(ctx, owner)
	text += "\n" + s.ContactStatusText(ctx, owner)
	if owner == s.Config.OwnerID {
		text += "\nБэкап: " + s.Archiver.BackupStatus(ctx)
	}
	if s.Watcher != nil {
		text += "\n" + s.Watcher.Status(ctx, owner)
	}
	if job, err := s.Store.CrawlStatus(ctx, owner); err == nil && job != nil {
		text += "\n" + job.shortStatus()
	}
	if s.Crawler != nil {
		if daily := s.Crawler.DailyStatus(ctx, owner); daily != "" {
			text += "\n" + daily
		}
	}
	_ = s.say(ctx, owner, text, nil)
}
func (s *Service) Fetch(ctx context.Context, owner int64) (int, error) {
	if err := s.Media.CheckDisk(); err != nil {
		return 0, err
	}
	profiles, err := s.Client.Users(ctx, owner, s.Store.Page(ctx, owner))
	if err != nil {
		return 0, err
	}
	if _, err = s.Store.Archive(ctx, owner, profiles, "GET /user/getUsers"); err != nil {
		return 0, err
	}
	if err = s.Store.RestoreFilteredBatch(ctx, owner, profiles); err != nil {
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
			if err := s.OfferOtherProfiles(ctx, owner); err != nil {
				_ = s.say(ctx, owner, "Не удалось предложить дополнительную выдачу. Попробуй позже.", nil)
			}
			return
		}
		p, err = s.Store.Next(ctx, owner)
	}
	if err != nil {
		_ = s.say(ctx, owner, "В этой пачке нет ещё не показанных анкет. Они сохранены в архиве; попробуй /next позже.", nil)
		return
	}
	fullCached, cacheErr := s.Store.ProfileHasFullCapture(ctx, owner, p.ID, p.VersionID)
	if cacheErr != nil {
		slog.Warn("full profile cache check failed", "tenant", owner, "kind", safeError(cacheErr))
	} else if !fullCached {
		if full, e := s.Client.FullProfile(ctx, owner, p.ID); e == nil {
			if _, e = s.Store.Archive(ctx, owner, []Profile{full}, "GET /user/getUser"); e == nil {
				p, _ = s.Store.Profile(ctx, owner, p.ID)
			}
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
	name := strings.Join(strings.Fields(v.String("name")+" "+v.String("surName")), " ")
	b.WriteString(name)
	age, height := v.String("age"), v.String("height")
	if age == "0" {
		age = ""
	}
	if height == "0" {
		height = ""
	}
	if age != "" || height != "" {
		b.WriteString(", " + strings.Trim(age+"/"+height, "/"))
	}
	var location []string
	for _, key := range []string{"country", "city"} {
		if val := profileValue(v, key); val != "" {
			location = append(location, val)
		}
	}
	if len(location) > 0 {
		b.WriteString("\n📍 " + strings.Join(location, ", "))
	}
	if birthday := profileBirthday(v); birthday != "" {
		b.WriteString("\n🎂 " + birthday)
	}
	for _, f := range []struct{ key, icon string }{{"goal", "🎯 "}, {"worldview", "🙏 "}, {"children", "👶 "}, {"profession", "💼 "}, {"languages", "🗣 "}} {
		if v.String(f.key) == "dont_tell" {
			continue
		}
		if val := profileValue(v, f.key); val != "" {
			if f.key == "children" && v.String("children") == "none" {
				val = "Детей нет"
			}
			b.WriteString("\n" + f.icon + val)
		}
	}
	if family := profileValue(v, "familyNames"); meaningfulProfileValue(family) {
		b.WriteString("\n👪 Родственные фамилии: " + family)
	}
	if bio := profileValue(v, "description"); meaningfulProfileValue(bio) {
		b.WriteString("\n\n" + bio)
	}
	var footer []string
	if fields := missingProfileFields(v); len(fields) > 0 {
		footer = append(footer, "Не указано: "+strings.Join(fields, ", "))
	}
	capturedAt := p.FirstSeen
	if capturedAt == "" {
		capturedAt = p.LastSeen
	}
	if capturedAt != "" {
		footer = append(footer, displayTime(capturedAt))
	}
	footer = append(footer, fmt.Sprintf("Jardulik: %d", p.ID))
	b.WriteString("\n\n" + strings.Join(footer, "\n"))
	return b.String()
}

// manualLike is called only after Handle verifies the private chat and locks its owner.
func (s *Service) manualLike(ctx context.Context, owner, id int64, guards ...func(context.Context) error) bool {
	if !s.Config.AllowReactions || !s.Store.cfg.AllowReactions {
		_ = s.say(ctx, owner, "Лайки сейчас выключены.", nil)
		return false
	}
	if s.Store.Paused(ctx, owner) {
		_ = s.say(ctx, owner, "Действия приостановлены: /resume", nil)
		return false
	}
	if _, err := s.Store.Profile(ctx, owner, id); err != nil {
		_ = s.say(ctx, owner, "Анкета недоступна.", nil)
		return false
	}
	// Avoid even an account refresh on a repeated click. Like still owns the
	// authoritative action claim, including races with other reaction callers.
	var state string
	err := s.Store.DB.QueryRowContext(ctx, `SELECT state FROM actions WHERE owner_id=? AND profile_id=? AND kind='Like' AND state IN ('pending','succeeded','unknown') LIMIT 1`, owner, id).Scan(&state)
	if err == nil {
		message := "Результат предыдущего лайка не подтверждён. Повторно не отправляю."
		if state == "succeeded" {
			message = "Лайк уже отправлен."
		}
		_ = s.say(ctx, owner, message, nil)
		return false
	}
	if !errors.Is(err, sql.ErrNoRows) {
		_ = s.say(ctx, owner, "Не удалось проверить предыдущие действия. Лайк не отправлен.", nil)
		return false
	}
	// LikeGuarded rechecks the personal budget around its fresh account check
	// and action claim, without enabling an automatic mode.
	result, err := s.Client.LikeGuarded(ctx, owner, id, func(guardCtx context.Context) error {
		for _, guard := range guards {
			if err := guard(guardCtx); err != nil {
				return err
			}
		}
		return s.ManualLikeBudgetGuard(guardCtx, owner)
	})
	if err != nil {
		_ = s.say(ctx, owner, s.manualLikeFailure(ctx, owner, id, err), nil)
		return false
	}
	message := "Лайк не подтверждён. Повторно не отправляю."
	switch result.Decision {
	case ReactionSuccess:
		message = "Лайк отправлен."
		if result.Mutual {
			message = "Взаимный лайк! Посмотреть: /matches"
		}
	case ReactionQuota:
		message = "Лимит лайков в Jardulik исчерпан."
		if result.Wait > 0 {
			message += fmt.Sprintf(" До следующего лайка: %d ч %d мин.", result.HoursLeft, result.MinutesLeft)
		}
	case ReactionFireUnavailable:
		message = "В Jardulik закончились реакции Огонь."
	case ReactionPremiumRequired:
		message = "Для этой реакции нужен Premium в Jardulik."
	case ReactionExhausted:
		message = "Анкеты закончились. Попробуйте позже."
	}
	if result.Exhausted && result.Decision != ReactionExhausted {
		message += " Анкеты закончились. Попробуйте позже."
	}
	_ = s.say(ctx, owner, message, nil)
	return result.Decision == ReactionSuccess
}

func (s *Service) manualLikeFailure(ctx context.Context, owner, id int64, err error) string {
	switch {
	case errors.Is(err, ErrAccountFrozen):
		return "Анкета заморожена в Jardulik. Лайк не отправлен."
	case errors.Is(err, ErrReactionDisabled):
		return "Лайки сейчас выключены."
	case errors.Is(err, ErrReactionPaused):
		return "Действия приостановлены: /resume"
	case errors.Is(err, ErrReactionSessionChanged):
		return "Сессия Jardulik изменилась. Лайк не отправлен. Проверь /status."
	case errors.Is(err, ErrOwnLikeLimit):
		return "Достигнут твой лимит отправок за 24 часа. Настройки: /settings."
	}
	var blocked *ActionBlockedError
	if errors.As(err, &blocked) {
		if blocked.State == "quota_waiting" {
			message := "Лимит лайков в Jardulik исчерпан."
			if wait := blocked.RetryAt.Sub(s.Client.Now()); !blocked.RetryAt.IsZero() && wait > 0 {
				minutes := int64(wait / time.Minute)
				if wait%time.Minute >= 30*time.Second {
					minutes++
				}
				if minutes == 0 {
					message += " Осталось меньше минуты."
				} else {
					message += fmt.Sprintf(" Осталось примерно %d ч %d мин.", minutes/60, minutes%60)
				}
			}
			return message
		}
		if blocked.State == "succeeded" {
			return "Лайк уже отправлен."
		}
		return "Эта реакция уже обрабатывалась. Повторно не отправляю."
	}
	// No action row means the fresh account check failed before sending.
	var state string
	if e := s.Store.DB.QueryRowContext(ctx, "SELECT state FROM actions WHERE owner_id=? AND profile_id=? AND kind='Like' ORDER BY id DESC LIMIT 1", owner, id).Scan(&state); errors.Is(e, sql.ErrNoRows) {
		return "Лайк не отправлен. Проверь аккаунт через /status."
	}
	return "Лайк не подтверждён. Повторно не отправляю."
}

func (s *Service) Deliver(ctx context.Context, owner int64, p StoredProfile) error {
	if _, err := s.Store.Version(ctx, owner, p.VersionID); err != nil {
		return err
	}
	pin, pinErr := s.keyboardSessionPin(ctx, owner)
	if err := s.clearActiveCard(ctx, owner); err != nil {
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
	ids, err = s.sendCurrentProfileCard(ctx, owner, p)
	if err != nil {
		return err
	}
	state = "sent"
	writeCtx, cancelWrite := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancelWrite()
	if pinErr == nil {
		if err := s.setActiveCard(writeCtx, owner, p, pin, ids); err != nil {
			slog.Warn("active keyboard card not set", "kind", safeError(err))
		}
	}
	_ = s.Store.SetMeta(writeCtx, fmt.Sprintf("card_layout:%d", deliveryID), "v2")
	s.Store.Delivered(writeCtx, owner, p.ID)
	s.Store.Event(writeCtx, owner, "card_delivered")
	return nil
}
func (s *Service) list(ctx context.Context, owner int64, favorites bool, before int64) {
	profiles, err := s.Store.List(ctx, owner, favorites, before)
	if err != nil {
		return
	}
	title, prefix := "История анкет", "lh"
	if favorites {
		title, prefix = "Избранное", "lf"
	}
	if len(profiles) == 0 {
		_ = s.say(ctx, owner, title+": пока пусто. /next покажет анкеты.", mainButtons())
		return
	}
	more := len(profiles) > 10
	if more {
		profiles = profiles[:10]
	}
	var buttons [][]map[string]string
	for _, p := range profiles {
		label := truncateRunes(profileHeading(p.Profile), 64)
		if label == "" {
			label = fmt.Sprintf("Анкета #%d", p.ID)
		}
		buttons = append(buttons, []map[string]string{{"text": label, "callback_data": fmt.Sprintf("p:%d", p.ID)}, {"text": "История", "callback_data": fmt.Sprintf("h:%d", p.ID)}})
	}
	var navigation []map[string]string
	if before > 0 {
		navigation = append(navigation, map[string]string{"text": "К началу", "callback_data": prefix + ":0"})
	}
	if more {
		navigation = append(navigation, map[string]string{"text": "Ещё", "callback_data": fmt.Sprintf("%s:%d", prefix, profiles[len(profiles)-1].Cursor)})
	}
	if len(navigation) > 0 {
		buttons = append(buttons, navigation)
	}
	_ = s.say(ctx, owner, title+"\nВыбери анкету:", map[string]any{"inline_keyboard": buttons})
}

func (s *Service) history(ctx context.Context, owner, id int64) {
	a, err := s.Store.History(ctx, owner, id)
	if err != nil || len(a) == 0 {
		_ = s.say(ctx, owner, "В твоём архиве такой анкеты нет.", nil)
		return
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Анкета #%d: %s\nПоказаны последние снимки: %d\n", id, a[0].Profile.String("name"), len(a))
	for i, p := range a {
		fmt.Fprintf(&b, "\n%s", displayTime(p.LastSeen))
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
	if err := s.DisableAutolike(context.Background(), owner, "stopped"); err != nil {
		slog.Warn("autolike stop unavailable", "kind", safeError(err))
	}
}

// WriteRecoveryKey keeps the off-server restore key out of Git and service archives.
func WriteRecoveryKey(c Config, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	return os.WriteFile(path, []byte("Jardulik Companion recovery key\nBACKUP_KEY="+base64.StdEncoding.EncodeToString(c.BackupKey)+"\nСохрани этот ключ отдельно от архивной группы. Без него сервисный архив нельзя расшифровать.\n"), 0600)
}

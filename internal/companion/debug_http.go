package companion

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const debugResponseLimit = 2 << 20

type debugHTTP struct {
	service *Service
	nonce   string
}

func newDebugHTTP(s *Service) (*debugHTTP, error) {
	u, err := url.Parse(s.Client.Base)
	if err != nil || u.Scheme != "https" || u.Host != "api.jardulik.app" || u.Path != "/api" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return nil, errors.New("debug API requires the configured Jardulik HTTPS origin")
	}
	nonce := make([]byte, 32)
	if _, err = rand.Read(nonce); err != nil {
		return nil, err
	}
	if _, err = s.Store.DB.Exec(debugAuditSchema); err != nil {
		return nil, err
	}
	if _, err = s.Store.DB.Exec("UPDATE debug_api_calls SET state='unknown',error='process_restarted',updated_at=CURRENT_TIMESTAMP WHERE state='sending'"); err != nil {
		return nil, err
	}
	return &debugHTTP{s, base64.RawURLEncoding.EncodeToString(nonce)}, nil
}

func (s *Service) prepareDebugHTTP() (func(context.Context), error) {
	if s.Config.DebugListen == "" {
		return nil, nil
	}
	host, port, err := net.SplitHostPort(s.Config.DebugListen)
	n, e := strconv.Atoi(port)
	if err != nil || host != "127.0.0.1" || e != nil || n < 0 || n > 65535 {
		return nil, errors.New("debug API requires loopback")
	}
	handler, err := newDebugHTTP(s)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", s.Config.DebugListen)
	if err != nil {
		return nil, err
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 100 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	return func(ctx context.Context) { runDebugHTTP(ctx, server, listener) }, nil
}

func runDebugHTTP(ctx context.Context, server *http.Server, listener net.Listener) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	server.BaseContext = func(net.Listener) context.Context { return runCtx }
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-runCtx.Done()
		// Request cancellation stops HTTP; joining handlers preserves their durable audit before Store closes.
		_ = server.Shutdown(context.Background())
	}()
	_ = server.Serve(listener)
	cancel()
	<-shutdownDone
}

func debugError(w http.ResponseWriter, status int, kind string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": kind})
}
func debugLocalHost(host string) bool {
	h, _, e := net.SplitHostPort(host)
	if e != nil {
		h = host
	}
	return h == "localhost" || h == "127.0.0.1" || h == "[::1]" || h == "::1"
}
func (d *debugHTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	if !debugLocalHost(r.Host) {
		debugError(w, 403, "local_host_required")
		return
	}
	origins := r.Header.Values("Origin")
	if len(origins) > 1 {
		debugError(w, 403, "same_origin_required")
		return
	}
	if origin := r.Header.Get("Origin"); origin != "" {
		u, e := url.Parse(origin)
		if e != nil || u.Scheme != "http" || u.Host != r.Host || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
			debugError(w, 403, "same_origin_required")
			return
		}
	}
	if r.Method == http.MethodGet && r.URL.Path == "/" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'nonce-"+d.nonce+"' https://kostia7alania.ru; style-src https://kostia7alania.ru 'unsafe-inline'; connect-src 'self' https://kostia7alania.ru; img-src data: https://kostia7alania.ru; font-src https://kostia7alania.ru; base-uri 'none'; frame-ancestors 'none'")
		_, _ = io.WriteString(w, strings.ReplaceAll(debugHTML, "__NONCE__", d.nonce))
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/debug/context" {
		ss, e := d.service.Store.Session(r.Context(), d.service.Config.OwnerID)
		w.Header().Set("Content-Type", "application/json")
		connected := e == nil && ss.State == "connected" && !d.service.Store.Paused(r.Context(), d.service.Config.OwnerID)
		var id int64
		if e == nil {
			id = ss.UserID
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"csrf_token": d.nonce, "upstream_id": id, "connected": connected})
		return
	}
	if !strings.HasPrefix(r.URL.Path, "/api/") {
		debugError(w, 404, "not_found")
		return
	}
	tokens := r.Header.Values("X-Jardulik-Debug")
	if len(tokens) != 1 || subtle.ConstantTimeCompare([]byte(tokens[0]), []byte(d.nonce)) != 1 {
		debugError(w, 403, "debug_nonce_required")
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		debugError(w, 405, "method_not_allowed")
		return
	}
	if r.Header.Get("Sec-Fetch-Site") == "cross-site" {
		debugError(w, 403, "same_origin_required")
		return
	}
	s := d.service
	s.beginForeground()
	defer s.endForeground()
	mu := s.userLock(s.Config.OwnerID)
	mu.Lock()
	defer mu.Unlock()
	ss, e := s.Store.Session(r.Context(), s.Config.OwnerID)
	if e != nil || ss.State != "connected" || s.Store.Paused(r.Context(), s.Config.OwnerID) {
		debugError(w, 409, "source_session_unavailable")
		return
	}
	if r.Method == http.MethodGet {
		d.get(w, r, ss)
		return
	}
	d.post(w, r, ss)
}

func debugOwnQuery(q url.Values, key string, id int64) error {
	if values, ok := q[key]; ok {
		if len(values) != 1 {
			return errors.New("ambiguous_owner_id")
		}
		n, e := strconv.ParseInt(values[0], 10, 64)
		if e != nil || n != id {
			return errors.New("owner_id_mismatch")
		}
	}
	q.Set(key, strconv.FormatInt(id, 10))
	return nil
}
func debugTarget(q url.Values, key string) (int64, error) {
	v := q[key]
	if len(v) != 1 {
		return 0, errors.New("target_required")
	}
	id, e := strconv.ParseInt(v[0], 10, 64)
	if e != nil || id <= 0 {
		return 0, errors.New("invalid_target")
	}
	return id, nil
}
func debugQueryKeys(q url.Values, keys ...string) bool {
	allowed := map[string]bool{}
	for _, k := range keys {
		allowed[k] = true
	}
	for k, v := range q {
		if !allowed[k] || len(v) != 1 {
			return false
		}
	}
	return true
}
func (d *debugHTTP) known(ctx context.Context, ss Session, id int64) error {
	if id == ss.UserID {
		return nil
	}
	return d.service.Store.KnownProfile(ctx, d.service.Config.OwnerID, id)
}
func (d *debugHTTP) get(w http.ResponseWriter, r *http.Request, ss Session) {
	path := strings.TrimPrefix(r.URL.Path, "/api")
	q := r.URL.Query()
	var err error
	switch path {
	case "/user/getMe":
		if !debugQueryKeys(q, "userId") {
			debugError(w, 400, "unsupported_query")
			return
		}
		err = debugOwnQuery(q, "userId", ss.UserID)
	case "/user/getUser":
		if !debugQueryKeys(q, "viewerUserId", "targetUserId") {
			debugError(w, 400, "unsupported_query")
			return
		}
		err = debugOwnQuery(q, "viewerUserId", ss.UserID)
		if err == nil {
			var id int64
			id, err = debugTarget(q, "targetUserId")
			if err == nil {
				err = d.known(r.Context(), ss, id)
			}
		}
	case "/user/getUsers":
		if !debugQueryKeys(q, "userId", "pageNumber", "pageSize", "all", "ageMin", "ageMax", "heightMin", "heightMax", "goal", "children", "worldview", "country", "city", "isMan") {
			debugError(w, 400, "unsupported_query")
			return
		}
		err = debugOwnQuery(q, "userId", ss.UserID)
	case "/user/getReactUsers":
		if !debugQueryKeys(q, "userId", "pageNumber", "pageSize", "isMutual") {
			debugError(w, 400, "unsupported_query")
			return
		}
		err = debugOwnQuery(q, "userId", ss.UserID)
	case "/orders/type":
		if !debugQueryKeys(q, "userId", "type") {
			debugError(w, 400, "unsupported_query")
			return
		}
		err = debugOwnQuery(q, "userId", ss.UserID)
	default:
		if !strings.HasPrefix(path, "/photos/user/") || len(q) != 0 {
			debugError(w, 404, "endpoint_not_allowed")
			return
		}
		id, e := strconv.ParseInt(strings.TrimPrefix(path, "/photos/user/"), 10, 64)
		if e != nil || id != ss.UserID {
			debugError(w, 400, "owner_id_mismatch")
			return
		}
	}
	if err != nil {
		debugError(w, 400, "invalid_owner_or_target")
		return
	}
	if v := q.Get("pageSize"); v != "" {
		n, e := strconv.Atoi(v)
		limit := 150
		if path == "/user/getReactUsers" {
			limit = 1000
		}
		if e != nil || n < 1 || n > limit {
			debugError(w, 400, "invalid_page_size")
			return
		}
	}
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	status, body, e := d.service.Client.debugRead(r.Context(), d.service.Config.OwnerID, ss, path)
	if e != nil && status == 0 {
		debugError(w, 502, "upstream_unavailable")
		return
	}
	if e == nil && (strings.HasPrefix(path, "/user/getMe?") || strings.HasPrefix(path, "/user/getUser?")) {
		profile, decodeErr := debugProfile(body)
		expected := ss.UserID
		if strings.HasPrefix(path, "/user/getUser?") {
			expected, _ = strconv.ParseInt(q.Get("targetUserId"), 10, 64)
		}
		if decodeErr != nil || profile.ID() != expected {
			debugError(w, 502, "unexpected_profile_identity")
			return
		}
	}
	if e == nil && strings.HasPrefix(path, "/photos/user/") {
		var photos []struct {
			UserID int64 `json:"userId"`
		}
		if json.Unmarshal(body, &photos) != nil {
			debugError(w, 502, "unexpected_photo_response")
			return
		}
		for _, photo := range photos {
			if photo.UserID != ss.UserID {
				debugError(w, 502, "unexpected_photo_identity")
				return
			}
		}
	}
	d.reply(w, status, body, ss)
}

func debugObject(raw []byte, allowed ...string) (map[string]json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	token, e := dec.Token()
	if e != nil || token != json.Delim('{') {
		return nil, errors.New("object_required")
	}
	out := map[string]json.RawMessage{}
	names := map[string]bool{}
	for _, k := range allowed {
		names[k] = true
	}
	for dec.More() {
		t, e := dec.Token()
		key, ok := t.(string)
		if e != nil || !ok || !names[key] {
			return nil, errors.New("unsupported_field")
		}
		if _, ok = out[key]; ok {
			return nil, errors.New("duplicate_field")
		}
		var value json.RawMessage
		if e = dec.Decode(&value); e != nil {
			return nil, e
		}
		out[key] = value
	}
	if _, e = dec.Token(); e != nil {
		return nil, e
	}
	var trailing any
	if dec.Decode(&trailing) != io.EOF {
		return nil, errors.New("one_object_required")
	}
	return out, nil
}
func debugJSONID(fields map[string]json.RawMessage, key string, optional bool) (int64, error) {
	raw, ok := fields[key]
	if !ok && optional {
		return 0, nil
	}
	var id int64
	if !ok || json.Unmarshal(raw, &id) != nil || id <= 0 {
		return 0, errors.New("invalid_id")
	}
	return id, nil
}
func (d *debugHTTP) post(w http.ResponseWriter, r *http.Request, ss Session) {
	if r.URL.RawQuery != "" || !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		debugError(w, 400, "json_body_required")
		return
	}
	requestID := r.Header.Get("X-Jardulik-Request-Id")
	if len(r.Header.Values("X-Jardulik-Request-Id")) != 1 || len(requestID) < 16 || len(requestID) > 128 || strings.IndexFunc(requestID, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_')
	}) >= 0 {
		debugError(w, 400, "request_id_required")
		return
	}
	raw, e := io.ReadAll(io.LimitReader(r.Body, 1<<20+1))
	if e != nil || len(raw) > 1<<20 {
		debugError(w, 413, "body_too_large")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api")
	var target int64
	var body []byte
	var reaction int64
	if path == "/GPT/checkCompatibility" {
		f, e := debugObject(raw, "fromUserId", "targetUserId")
		if e != nil {
			debugError(w, 400, "invalid_request")
			return
		}
		target, e = debugJSONID(f, "targetUserId", false)
		if e != nil {
			debugError(w, 400, "invalid_target")
			return
		}
		if v, ok := f["fromUserId"]; ok {
			var id int64
			if json.Unmarshal(v, &id) != nil || id != ss.UserID {
				debugError(w, 400, "owner_id_mismatch")
				return
			}
		}
		body, _ = json.Marshal(map[string]int64{"fromUserId": ss.UserID, "targetUserId": target})
	} else if path == "/reactions/addNew" {
		if !d.service.Config.AllowReactions {
			debugError(w, 409, "reactions_disabled")
			return
		}
		f, e := debugObject(raw, "fromUserId", "userId", "reactionType", "isDeleted", "message")
		if e != nil {
			debugError(w, 400, "invalid_request")
			return
		}
		target, e = debugJSONID(f, "userId", false)
		if e != nil {
			debugError(w, 400, "invalid_target")
			return
		}
		if v, ok := f["fromUserId"]; ok {
			var id int64
			if json.Unmarshal(v, &id) != nil || id != ss.UserID {
				debugError(w, 400, "owner_id_mismatch")
				return
			}
		}
		if json.Unmarshal(f["reactionType"], &reaction) != nil || reaction < 1 || reaction > 4 {
			debugError(w, 400, "unknown_reaction_type")
			return
		}
		deleted := reaction == 4
		if v, ok := f["isDeleted"]; ok {
			var value bool
			if json.Unmarshal(v, &value) != nil || value != deleted {
				debugError(w, 400, "inconsistent_deleted_flag")
				return
			}
		}
		message := ""
		if v, ok := f["message"]; ok {
			if !utf8.Valid(v) || json.Unmarshal(v, &message) != nil {
				debugError(w, 400, "invalid_message")
				return
			}
		}
		if reaction != 1 {
			message = strings.TrimSpace(message)
		}
		if validateReactionMessage(message) != nil || reaction == 3 && message == "" || reaction != 1 && reaction != 3 && message != "" {
			debugError(w, 400, "invalid_reaction_message")
			return
		}
		body, _ = json.Marshal(map[string]any{"fromUserId": ss.UserID, "userId": target, "reactionType": reaction, "isDeleted": deleted, "message": message})
	} else {
		debugError(w, 404, "endpoint_not_allowed")
		return
	}
	if e = d.known(r.Context(), ss, target); e != nil {
		debugError(w, 400, "unknown_target")
		return
	}
	d.write(w, r, ss, requestID, path, body, target, reaction)
}

func (d *debugHTTP) reply(w http.ResponseWriter, status int, body []byte, ss Session) {
	if d.hasSecret(body, ss) {
		debugError(w, 502, "credential_in_upstream_response")
		return
	}
	if status < 100 || status > 599 {
		debugError(w, 502, "upstream_unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

const debugHTML = `<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex"><title>Jardulik · мой API-клиент</title><link rel="stylesheet" href="https://kostia7alania.ru/jardulik-companion-site/swagger/vendor/swagger-ui.css" integrity="sha384-4c3vsE5f+trhFyHrFrLM1fFGUn531NIaC7dcqEWIJdoRQJ5CkJbSuSCwXLpaS1KF" crossorigin="anonymous" referrerpolicy="no-referrer"><link rel="stylesheet" href="https://kostia7alania.ru/jardulik-companion-site/swagger/swagger.css"></head><body><header class="docs-header"><h1>Jardulik · мой API-клиент</h1><p id="debug-state">Подключение…</p></header><main class="docs-main"><div id="swagger-ui"></div></main><script src="https://kostia7alania.ru/jardulik-companion-site/swagger/vendor/swagger-ui-bundle.js" integrity="sha384-XFYN47VzPiDY6qPtxYKg0nqUeW/upo20xf1j2SUE7gT/i35FJB4cuS/71Iegzk8Z" crossorigin="anonymous" referrerpolicy="no-referrer"></script><script nonce="__NONCE__">
(async()=>{try{
 const context=await fetch('/debug/context',{cache:'no-store'}).then(r=>r.json());
 document.getElementById('debug-state').textContent=context.connected?'Аккаунт '+context.upstream_id+' подключён. Заголовки добавляет бэк.':'Подключи Jardulik в Telegram-боте.';
 const spec=await fetch('https://kostia7alania.ru/jardulik-companion-site/swagger/openapi.json',{credentials:'omit'}).then(r=>r.json());
 spec.servers=[{url:location.origin+'/api',description:'Мой бэк'}];delete spec.security;if(spec.components)delete spec.components.securitySchemes;
 const deref=p=>p?.$ref?.startsWith('#/')?p.$ref.slice(2).split('/').reduce((v,k)=>v?.[k.replace(/~1/g,'/').replace(/~0/g,'~')],spec)||p:p;
 const params=ps=>(ps||[]).map(deref).filter(p=>!(p.in==='query'&&['userId','viewerUserId'].includes(p.name)));
 for(const item of Object.values(spec.paths)){if(item.parameters)item.parameters=params(item.parameters);for(const[method,op]of Object.entries(item)){if(!['get','post','put','patch','delete','head','options'].includes(method)||typeof op!=='object'||op===null)continue;delete op.security;if(op.parameters)op.parameters=params(op.parameters);}}
 for(const schema of Object.values(spec.components?.schemas||{})){if(schema.properties?.fromUserId){delete schema.properties.fromUserId;if(schema.required)schema.required=schema.required.filter(k=>k!=='fromUserId');}}
 const photo=spec.paths['/photos/user/{userId}'];if(photo){delete spec.paths['/photos/user/{userId}'];if(photo.parameters)photo.parameters=photo.parameters.map(deref).filter(p=>p.name!=='userId');for(const op of Object.values(photo)){if(op&&typeof op==='object'&&op.parameters)op.parameters=op.parameters.map(deref).filter(p=>p.name!=='userId');}spec.paths['/photos/user/'+context.upstream_id]=photo;}
 SwaggerUIBundle({spec,dom_id:'#swagger-ui',deepLinking:true,filter:true,validatorUrl:null,persistAuthorization:false,queryConfigEnabled:false,tryItOutEnabled:false,syntaxHighlight:{theme:'monokai'},requestInterceptor(request){const u=new URL(request.url,location.href);if(u.origin!==location.origin||!u.pathname.startsWith('/api/'))throw new Error('Unexpected API destination');request.headers=request.headers||{};delete request.headers.Authorization;delete request.headers.authorization;request.headers['X-Jardulik-Debug']=context.csrf_token;if((request.method||'GET').toUpperCase()!=='GET')request.headers['X-Jardulik-Request-Id']=crypto.randomUUID();return request;}});
}catch(error){document.getElementById('debug-state').textContent='Не удалось загрузить Swagger. Обнови страницу.';}})();
</script></body></html>`

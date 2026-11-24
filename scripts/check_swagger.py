#!/usr/bin/env python3
"""Validate the published reference and its pinned static Swagger distribution."""
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlsplit, unquote
import base64
import hashlib
import json
import re

ROOT = Path(__file__).resolve().parents[1]
WEB = ROOT / 'internal' / 'companion' / 'web'
DOCS = WEB / 'swagger'
CANONICAL = 'https://kostia7alania.ru/jardulik-companion/swagger/'
SOURCE_LINK = 'https://github.com/kostia7alania/jardulik-companion/blob/main/internal/companion/web/swagger/openapi.json'
spec = json.loads((DOCS / 'openapi.json').read_text())
assert spec['openapi'] == '3.0.3'
assert spec['servers'] == [{'url': 'https://api.jardulik.app/api', 'description': 'Официальный Jardulik API'}]
assert spec['security'] == [{'BearerAuth': []}]
assert spec['components']['securitySchemes']['BearerAuth'] == {'type': 'http', 'scheme': 'bearer', 'bearerFormat': 'JWT'}
expected_paths = {'/user/getMe', '/photos/user/{userId}', '/user/getUser', '/user/getUsers', '/user/getReactUsers', '/orders/type', '/GPT/checkCompatibility', '/reactions/addNew'}
assert set(spec['paths']) == expected_paths
operations = []
for path, item in spec['paths'].items():
    for method, operation in item.items():
        assert method in {'get', 'post'}, (path, method)
        assert operation['summary'] and operation['description']
        assert operation['responses']['200']
        operations.append(operation['operationId'])
        parameters = operation.get('parameters', [])
        assert len({(p['in'], p['name']) for p in parameters}) == len(parameters)
        for p in parameters:
            if p['in'] == 'path':
                assert '{' + p['name'] + '}' in path and p['required']
assert len(operations) == len(set(operations)) == 8

def walk(value):
    if isinstance(value, dict):
        if '$ref' in value:
            reference = value['$ref']
            assert reference.startswith('#/'), reference
            target = spec
            for part in reference[2:].split('/'):
                target = target[part.replace('~1', '/').replace('~0', '~')]
        for child in value.values():
            walk(child)
    elif isinstance(value, list):
        for child in value:
            walk(child)
walk(spec)
empty_profile = spec['paths']['/user/getUser']['get']['responses']['204']
assert empty_profile == {'description': 'Нет содержимого анкеты.'}
assert 'content' not in empty_profile
assert all('204' not in item[method]['responses'] for path, item in spec['paths'].items() if path != '/user/getUser' for method in item)
schemas = spec['components']['schemas']
assert schemas['ReactionRequest']['properties']['reactionType']['enum'] == [1, 2, 3, 4]
assert 'UTF-16' in schemas['ReactionRequest']['properties']['message']['description']
assert set(schemas['GPTRequest']['required']) == {'fromUserId', 'targetUserId'}
assert schemas['GPTRequest']['additionalProperties'] is False
assert set(schemas['GPTRequest']['properties']) == {'fromUserId', 'targetUserId'}
assert schemas['ReactionRequest']['additionalProperties'] is False
assert set(schemas['ReactionRequest']['properties']) == {'reactionType', 'isDeleted', 'userId', 'fromUserId', 'message'}
assert set(schemas['GPTResponse']['properties']) == {'content', 'model'}
assert 'format' not in schemas['Photo']['properties']['createDate']
assert 'isMan' not in {p['name'] for p in spec['paths']['/user/getUsers']['get']['parameters']}
assert schemas['OrderTypes']['type'] == 'array'
assert schemas['OrderTypes']['items'] == {'$ref': '#/components/schemas/OrderType'}
assert set(schemas['OrderType']['properties']) == {'orderId', 'type', 'charId', 'currency', 'price', 'originalPrice', 'discountPercent', 'isActive', 'fire', 'period', 'clean', 'createDate'}
assert schemas['OrderType']['properties']['originalPrice']['nullable'] is True
assert 'format' not in schemas['OrderType']['properties']['createDate']

class HTML(HTMLParser):
    def __init__(self):
        super().__init__()
        self.sources = []
        self.scripts = []
        self.sri = {}
        self.h1 = 0
        self.color_scheme = False
        self.canonicals = []
        self.noindex = False
        self.local_mode = False
    def handle_starttag(self, tag, pairs):
        attrs = dict(pairs)
        if tag == 'h1':
            self.h1 += 1
        if tag == 'meta' and attrs.get('name') == 'color-scheme':
            self.color_scheme = attrs.get('content') == 'dark'
        if tag == 'meta' and attrs.get('name') == 'robots':
            self.noindex = 'noindex' in attrs.get('content', '')
        if tag == 'a' and attrs.get('href') == 'http://127.0.0.1:8788/':
            self.local_mode = True
        if tag == 'link' and attrs.get('rel') == 'canonical':
            self.canonicals.append(attrs.get('href'))
        if tag == 'script':
            self.scripts.append(attrs.get('src', ''))
        for key in ('src', 'href'):
            value = attrs.get(key)
            if value:
                self.sources.append(value)
                if 'integrity' in attrs:
                    assert attrs.get('crossorigin') == 'anonymous'
                    assert attrs.get('referrerpolicy') == 'no-referrer'
                    self.sri[value] = attrs['integrity']
page = HTML()
page.feed((DOCS / 'index.html').read_text())
assert page.h1 == 1 and page.color_scheme and page.noindex and page.local_mode
assert page.canonicals == [CANONICAL], page.canonicals
assert '../' in page.sources and SOURCE_LINK in page.sources
assert page.scripts == ['vendor/swagger-ui-bundle.js', 'swagger.js']
for source in page.sources:
    parsed = urlsplit(source)
    if parsed.scheme or parsed.netloc:
        continue
    target = (DOCS / unquote(parsed.path)).resolve()
    if target.is_dir():
        target /= 'index.html'
    assert target.is_relative_to(WEB) and target.is_file(), source
manifest = json.loads((DOCS / 'vendor/provenance.json').read_text())
assert manifest['version'] == '5.34.1' and manifest['package'] == 'swagger-ui-dist'
for name, metadata in manifest['files'].items():
    data = (DOCS / 'vendor' / name).read_bytes()
    assert len(data) == metadata['bytes']
    assert hashlib.sha256(data).hexdigest() == metadata['sha256']
    sri = 'sha384-' + base64.b64encode(hashlib.sha384(data).digest()).decode()
    assert sri == metadata['sha384_sri']
for name in ('swagger-ui-bundle.js', 'swagger-ui.css'):
    assert page.sri['vendor/' + name] == manifest['files'][name]['sha384_sri']
for name in ('LICENSE', 'NOTICE', 'swagger-ui-bundle.js.LICENSE.txt'):
    assert (DOCS / 'vendor' / name).is_file()
js = (DOCS / 'swagger.js').read_text()
for required in (
    'supportedSubmitMethods: []', 'tryItOutEnabled: false', 'validatorUrl: null',
    'queryConfigEnabled: false', 'persistAuthorization: false', 'withCredentials: false',
    "credentials: 'omit'",
):
    assert required in js, required
assert '/debug/context' not in js
css = (DOCS / 'swagger.css').read_text()
assert 'color-scheme:dark' in css and ':focus-visible' in css
for name in ('index.html', 'openapi.json', 'swagger.js', 'swagger.css'):
    source = (DOCS / name).read_text()
    assert not re.search(r'tgWebAppData=|eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}|\d{8,}:[A-Za-z0-9_-]{30,}', source), name
landing = HTML()
landing.feed((WEB / 'index.html').read_text())
assert './swagger/' in landing.sources and './swagger/openapi.json' in landing.sources
print('OK: 8 OpenAPI operations, local refs, pinned Swagger UI integrity/licenses, public read-only mode, private-client link, authored asset boundaries')

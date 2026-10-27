import asyncio
import hashlib
import json
import logging
import os
import re
import time
import hmac
from contextlib import asynccontextmanager
from typing import Optional
from fastapi.encoders import jsonable_encoder

import asyncpg
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, model_validator

from regions import VALID_REGIONS
from validators import (
    RE_PERSON_NAME,
    RE_LEGAL_NAME,
    is_valid_inn,
    has_meaningful_word,
    normalize_party,
)
from selects import search_count, last_cases, explain_last_cases, explain_count


# ---------------------------------------------------------------------------
# Секреты из окружения (EnvironmentFile=/etc/api/api.env)
# ---------------------------------------------------------------------------
API_TOKEN   = os.environ['API_TOKEN']
DB_PASSWORD = os.environ['DB_PASSWORD']

# /debug/explain выполняет EXPLAIN ANALYZE (реально гоняет тяжёлые запросы мимо лимитов и квоты),
# поэтому по умолчанию выключен. Для отладки: DEBUG_ENDPOINTS=1 в /etc/api/api.env + рестарт.
DEBUG_ENDPOINTS = os.environ.get('DEBUG_ENDPOINTS') == '1'


# ---------------------------------------------------------------------------
# Подключение к Postgres (не-секреты — явно)
# ---------------------------------------------------------------------------
DB_HOST, DB_PORT = '127.0.0.1', 5432
DB_NAME, DB_USER = 'sudrf', 'postgres'


# ---------------------------------------------------------------------------
# Ограничители и лимиты
# ---------------------------------------------------------------------------
MAX_CONCURRENT   = 2      # максимум одновременных запросов (семафор), = POOL_MAX
POOL_MIN         = 1      # минимум соединений в пуле
POOL_MAX         = 2      # максимум соединений к Postgres (жёсткий потолок)
WAIT_FOR_SLOT    = 30     # сек: сколько ждать слот семафора, потом 503
DAILY_LIMIT      = 100    # квота: успешных SELECT в сутки на потребителя
RATE             = 0.2    # rate limit: токенов в секунду (0.2 = 1 токен за 5 с)
BURST            = 2      # rate limit: сколько запросов можно сразу (всплеск)
DENIAL_THRESHOLD = 5      # писать отказ в БД, когда за минуту набралось >= 5
COUNT_TTL        = 6 * 3600   # сек: сколько хранить count в памяти (сами кейсы всегда свежие)
COUNT_CACHE_MAX  = 5000       # максимум записей в кэше count


# ---------------------------------------------------------------------------
# Логи: JSON в stdout -> journald
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger('api')


# Глобальный семафор: не больше MAX_CONCURRENT корутин в критической секции.
# Работает только при --workers 1 (один процесс).
SEM = asyncio.Semaphore(MAX_CONCURRENT)


# Пул соединений создаётся в lifespan при старте приложения.
DB_POOL: Optional[asyncpg.Pool] = None


# Счётчики отказов в памяти: {(consumer, kind, minute_ts): count}
# Нужны, чтобы писать в БД только при >= DENIAL_THRESHOLD отказов за минуту.
_denial_counts: dict = {}


# Кэш count: {(имя, инн): (истекает_monotonic, count)}. Живёт в памяти процесса
# (работает при --workers 1), сбрасывается при рестарте.
_count_cache: dict = {}


# ---------------------------------------------------------------------------
# Токен-бакет для rate limit (in-memory, сбрасывается при рестарте)
# ---------------------------------------------------------------------------
class TokenBucket:
    def __init__(self, rate: float, burst: int):
        self.rate = rate          # токенов в секунду
        self.burst = burst        # максимум токенов (всплеск)
        self._buckets = {}        # key -> (tokens, last_monotonic_ts)

    def allow(self, key: str):
        """Возвращает (разрешено, retry_after_сек). Забирает 1 токен если можно."""
        now = time.monotonic()
        tokens, last = self._buckets.get(key, (float(self.burst), now))
        tokens = min(self.burst, tokens + (now - last) * self.rate)
        if tokens < 1.0:
            self._buckets[key] = (tokens, now)
            return False, (1.0 - tokens) / self.rate
        self._buckets[key] = (tokens - 1.0, now)
        return True, 0.0


RATE_LIMITER = TokenBucket(rate=RATE, burst=BURST)


def consumer_hash(token: str) -> str:
    """Хэш токена для записи в БД (не храним сам токен)."""
    return hashlib.sha256(token.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Валидация тела POST /search
# ---------------------------------------------------------------------------
class SearchBody(BaseModel):
    # forbid: любое неизвестное поле (например, add_name) -> 422,
    # клиент сразу видит опечатку, а не молчаливое игнорирование.
    model_config = ConfigDict(extra='forbid')

    is_firm:   bool                    # true = ЮЛ, false = ФЛ/ИП
    name:      str                     # ФИО (для ФЛ/ИП) или наименование (для ЮЛ)
    inn:       Optional[str] = None    # 10 цифр для ЮЛ, 12 для ИП; для ФЛ не нужен
    region_id: Optional[int] = None    # регион, опционально

    @model_validator(mode='after')
    def validate_by_type(self):
        name = (self.name or '').strip()

        if not self.is_firm:
            # --- ФЛ / ИП ---
            if len(name) > 50:
                raise ValueError('name must be at most 50 characters')

            if not RE_PERSON_NAME.match(name):
                raise ValueError(
                    'name must contain only Cyrillic letters, spaces, hyphens'
                )

            words = normalize_party(name).split()
            if not (2 <= len(words) <= 4):
                raise ValueError('name must be 2-4 words (ФИО)')

            for w in words:
                if len(w) < 2:
                    raise ValueError(
                        f'each word must be at least 2 characters, got {w!r}'
                    )

            if not has_meaningful_word(name):
                raise ValueError('name must contain a meaningful word')

            if self.inn is not None and self.inn != '':
                if not is_valid_inn(self.inn, 12):
                    raise ValueError(
                        'inn must be exactly 12 digits, not all identical'
                    )

        else:
            # --- ЮЛ ---
            if len(name) > 50:
                raise ValueError('name must be at most 50 characters')

            if not RE_LEGAL_NAME.match(name):
                raise ValueError(
                    'name contains characters not allowed for legal entity'
                )

            if not re.search(r'[А-Яа-яЁёA-Za-z]{3,}', name):
                raise ValueError(
                    'name must contain at least one word of 3+ letters'
                )

            if not has_meaningful_word(name):
                raise ValueError(
                    'name must contain a meaningful word (not only legal form)'
                )

            if not self.inn:
                raise ValueError('inn is required when is_firm=true')
            if not is_valid_inn(self.inn, 10):
                raise ValueError('inn must be exactly 10 digits, not all identical')

        # --- region_id ---
        if self.region_id is not None and self.region_id not in VALID_REGIONS:
            raise ValueError(
                f'region_id must be one of allowed regions, got {self.region_id}'
            )

        return self


# ---------------------------------------------------------------------------
# Старт/остановка приложения
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global DB_POOL
    DB_POOL = await asyncpg.create_pool(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASSWORD, database=DB_NAME,
        min_size=POOL_MIN, max_size=POOL_MAX,
        command_timeout=30, timeout=5,
        # Значения подставляются в SQL литералами -> каждый запрос уникален,
        # кэш prepared statements только засоряется.
        statement_cache_size=0,
        server_settings={
            'timezone': 'Asia/Yekaterinburg',   # «сутки» и date_trunc — по Екатеринбургу
            'jit': 'off',                       # JIT на COUNT(DISTINCT) часто замедляет
            'work_mem': '64MB',                 # для COUNT(DISTINCT) / hash aggregate
            'effective_io_concurrency': '100',  # опережающее чтение в Bitmap Heap Scan
        },
    )
    log.info(json.dumps({
        'event': 'startup', 'db': DB_NAME,
        'max_concurrent': MAX_CONCURRENT, 'pool_max': POOL_MAX,
        'rate': RATE, 'burst': BURST,
        'daily_limit': DAILY_LIMIT, 'denial_threshold': DENIAL_THRESHOLD,
    }))
    yield
    await DB_POOL.close()
    log.info(json.dumps({'event': 'shutdown'}))


app = FastAPI(
    lifespan=lifespan,
    docs_url=None,        # /docs закрыт
    redoc_url=None,       # /redoc закрыт
    openapi_url=None,     # /openapi.json закрыт
)


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------
async def seconds_until_midnight(conn) -> int:
    """Секунд до полуночи по TZ сессии (Asia/Yekaterinburg)."""
    return await conn.fetchval(
        """SELECT EXTRACT(EPOCH FROM (
               date_trunc('day', now()) + interval '1 day' - now()
           ))::int"""
    )


async def check_quota(conn, consumer: str):
    """Возвращает (можно_ещё, осталось). Считает успешные SELECT за сегодня."""
    used = await conn.fetchval(
        """SELECT count(*) FROM api_requests
           WHERE consumer = $1
             AND requested_at >= date_trunc('day', now())
             AND requested_at <  date_trunc('day', now()) + interval '1 day'""",
        consumer,
    )
    return used < DAILY_LIMIT, DAILY_LIMIT - used


async def record_denial_if_threshold(conn, consumer: str, kind: str, ip: str) -> int:
    """
    Считает отказы в памяти. Возвращает текущий count за минуту.
    В БД пишет 1 строку только когда count == DENIAL_THRESHOLD.
    При шторме (1000/сек) — 1 INSERT/мин максимум.
    """
    now = time.time()
    minute_ts = int(now // 60) * 60          # начало текущей минуты (unix)
    key = (consumer, kind, minute_ts)

    _denial_counts[key] = _denial_counts.get(key, 0) + 1
    count = _denial_counts[key]

    # Чистка старых минут, чтобы dict не рос бесконечно
    if len(_denial_counts) > 1000:
        cutoff = minute_ts - 300             # старше 5 минут — выкинуть
        for k in [k for k in list(_denial_counts) if k[2] < cutoff]:
            del _denial_counts[k]

    if count == DENIAL_THRESHOLD:
        await conn.execute(
            """INSERT INTO api_quota_denials (consumer, minute, kind, count, ip)
               VALUES ($1, to_timestamp($2), $3, 1, $4::inet)
               ON CONFLICT (consumer, minute, kind) DO NOTHING""",
            consumer, minute_ts, kind, ip,
        )
        log.info(json.dumps({
            'event': 'denial_recorded', 'consumer': consumer,
            'kind': kind, 'minute': minute_ts, 'count': count, 'ip': ip,
        }))

    return count


async def record_ok(conn, consumer: str, kind: str, ip: str, body: dict) -> int:
    """
    Запись успешного SELECT (идёт в квоту).
    Возвращает остаток квоты ПОСЛЕ вставки (сколько ещё можно за сегодня).
    """
    row = await conn.fetchrow(
        """
        WITH ins AS (
            INSERT INTO api_requests (consumer, kind, ip, body)
            VALUES ($1, $2, $3::inet, $4::jsonb)
            RETURNING 1
        )
        -- основной запрос не видит строку, вставленную в этом же выражении (одна копия снимка),
        -- поэтому только что записанный запрос добавляем отдельно: + (SELECT count(*) FROM ins)
        SELECT GREATEST(0, $5::int - (
            (SELECT count(*) FROM api_requests
             WHERE consumer = $1
               AND requested_at >= date_trunc('day', now())
               AND requested_at <  date_trunc('day', now()) + interval '1 day')
            + (SELECT count(*) FROM ins)
        )) AS left
        """,
        consumer, kind, ip, json.dumps(body), DAILY_LIMIT,
    )
    return row['left']


async def acquire_slot():
    """Ждёт слот семафора до WAIT_FOR_SLOT секунд, иначе 503."""
    if SEM.locked():
        log.info(json.dumps({'event': 'queue_wait'}))
    try:
        await asyncio.wait_for(SEM.acquire(), timeout=WAIT_FOR_SLOT)
    except asyncio.TimeoutError:
        raise HTTPException(status_code=503, detail='server busy')


async def cached_count(conn, name: str, inn: Optional[str], region_id: Optional[int] = None):
    """
    count с кэшем по (имя, инн, регион). Возвращает (count, из_кэша).
    Для крупных имён (Сбербанк и т.п.) подсчёт дорогой, а число меняется медленно.
    """
    key = (' '.join(name.lower().split()), inn or '', region_id)
    now = time.monotonic()

    hit = _count_cache.get(key)
    if hit and hit[0] > now:
        return hit[1], True

    count = await search_count(conn, name, inn, region_id)

    if len(_count_cache) >= COUNT_CACHE_MAX:
        for k in [k for k, v in _count_cache.items() if v[0] <= now]:
            del _count_cache[k]
        if len(_count_cache) >= COUNT_CACHE_MAX:
            _count_cache.pop(next(iter(_count_cache)))   # самый старый
    _count_cache[key] = (now + COUNT_TTL, count)
    return count, False


# ---------------------------------------------------------------------------
# Эндпоинты
# ---------------------------------------------------------------------------
@app.get('/health')
def health():
    """Открыт без токена и лимитов — для мониторинга."""
    return {'ok': True}


@app.post('/search')
async def search(body: SearchBody, request: Request):
    # 1. auth
    token = request.headers.get('x-api-key', '')
    if not token or not hmac.compare_digest(
        token.encode('utf-8', errors='replace'),
        API_TOKEN.encode('utf-8'),
    ):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)

    consumer = consumer_hash(token)
    ip = request.client.host if request.client else '0.0.0.0'

    # 2. rate limit (in-memory, до БД)
    allowed, retry_after = RATE_LIMITER.allow(token)
    if not allowed:
        async with DB_POOL.acquire() as conn:
            count = await record_denial_if_threshold(conn, consumer, 'rate_limited', ip)
        log.info(json.dumps({
            'event': 'rate_limited', 'consumer': consumer,
            'count_min': count, 'retry_after': round(retry_after, 2),
        }))
        return JSONResponse(
            {'error': 'rate limited', 'retry_after': round(retry_after, 2)},
            status_code=429,
            headers={'Retry-After': str(int(retry_after) + 1)},
        )

    body_dict = body.dict()

    # 3. квота (Postgres, до семафора)
    async with DB_POOL.acquire() as conn:
        ok, _ = await check_quota(conn, consumer)
        if not ok:
            count = await record_denial_if_threshold(conn, consumer, 'quota_exceeded', ip)
            retry = await seconds_until_midnight(conn)
            log.info(json.dumps({
                'event': 'quota_exceeded', 'consumer': consumer, 'count_min': count,
            }))
            return JSONResponse(
                {'error': 'quota exhausted', 'limit': DAILY_LIMIT},
                status_code=429,
                headers={
                    'Retry-After': str(retry),
                    'X-Quota-Remaining': '0',
                },
            )

    # 4. семафор (макс 2 одновременно)
    await acquire_slot()
    try:
        # 5. данные + запись в квоту (одно соединение на оба действия)
        async with DB_POOL.acquire() as conn:
            t0 = time.perf_counter()
            count, count_cached = await cached_count(conn, body.name, body.inn, body.region_id)
            t1 = time.perf_counter()
            rows = await last_cases(conn, body.name, body.inn, limit=5, region_id=body.region_id)
            t2 = time.perf_counter()
            left_after = await record_ok(conn, consumer, 'search', ip, body_dict)
        result = {'count': count, 'rows': rows}
    except (asyncio.TimeoutError, asyncpg.exceptions.QueryCanceledError):
        # запрос не уложился в лимит времени; квота не списывается (record_ok не выполнен)
        log.warning(json.dumps({
            'event': 'search_timeout', 'consumer': consumer, 'ip': ip,
            'is_firm': body.is_firm, 'has_inn': bool(body.inn), 'region_id': body.region_id,
        }))
        return JSONResponse({'error': 'query timeout'}, status_code=504)
    finally:
        SEM.release()

    log.info(json.dumps({
        'event': 'search_ok', 'consumer': consumer,
        'ip': ip, 'quota_left': left_after, 'count_cached': count_cached,
        'count_ms': round((t1 - t0) * 1000), 'cases_ms': round((t2 - t1) * 1000),
    }))
    return JSONResponse(
        jsonable_encoder({'ok': True, 'quota_left': left_after, 'result': result}),
        headers={'X-Quota-Remaining': str(left_after)},
    )


async def debug_explain(body: SearchBody, request: Request):
    # тот же auth, что у /search
    token = request.headers.get('x-api-key', '')
    if not token or not hmac.compare_digest(
        token.encode('utf-8', errors='replace'),
        API_TOKEN.encode('utf-8'),
    ):
        return JSONResponse({'error': 'unauthorized'}, status_code=401)

    # EXPLAIN ровно тех же запросов, что выполняет /search, в том же порядке:
    # сначала count, потом последние 5 кейсов. Квоту не тратит.
    async with DB_POOL.acquire() as conn:
        count_plans = await explain_count(conn, body.name, body.inn, body.region_id)
        cases_plan = await explain_last_cases(conn, body.name, body.inn, limit=5,
                                              region_id=body.region_id)

    return {
        'count_plans': {k: v.split('\n') for k, v in count_plans.items()},
        'plan': cases_plan.split('\n'),      # план last_cases (как и раньше)
    }


if DEBUG_ENDPOINTS:
    app.post('/debug/explain')(debug_explain)
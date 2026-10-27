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
from selects import search_count, last_cases


# ---------------------------------------------------------------------------
# Секреты из окружения (EnvironmentFile=/etc/api/api.env)
# ---------------------------------------------------------------------------
API_TOKEN   = os.environ['API_TOKEN']
DB_PASSWORD = os.environ['DB_PASSWORD']


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

            # формат проверяем на исходном name (до нормализации)
            if not RE_PERSON_NAME.match(name):
                raise ValueError(
                    'name must contain only Cyrillic letters, spaces, hyphens'
                )

            # слова считаем после нормализации (дефис -> пробел, ё->е),
            # чтобы совпадать с тем, как ищется в БД
            words = normalize_party(name).split()
            if not (2 <= len(words) <= 4):
                raise ValueError('name must be 2-4 words (ФИО)')

            for w in words:
                if len(w) < 2:
                    raise ValueError(
                        f'each word must be at least 2 characters, got {w!r}'
                    )

            # защита от абуза: хотя бы одно значимое слово (не ОПФ/служебное/«аааа»)
            if not has_meaningful_word(name):
                raise ValueError('name must contain a meaningful word')

            # ИНН ИП: is_firm=false + inn 12 цифр -> ИП; без inn -> обычный ФЛ
            if self.inn is not None and self.inn != '':
                if not is_valid_inn(self.inn, 12):
                    raise ValueError(
                        'inn must be exactly 12 digits, not all identical'
                    )

        else:
            # --- ЮЛ ---
            if len(name) > 50:
                raise ValueError('name must be at most 50 characters')

            # формат проверяем на исходном name (до нормализации)
            if not RE_LEGAL_NAME.match(name):
                raise ValueError(
                    'name contains characters not allowed for legal entity'
                )

            # минимум одно слово из 3+ букв (кириллица или латиница)
            if not re.search(r'[А-Яа-яЁёA-Za-z]{3,}', name):
                raise ValueError(
                    'name must contain at least one word of 3+ letters'
                )

            # защита от абуза: не только ОПФ/служебные, не «аааа»
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
        server_settings={
            'timezone': 'Asia/Yekaterinburg',   # «сутки» и date_trunc — по Екатеринбургу
            'jit': 'off',                    # JIT на COUNT(DISTINCT) часто замедляет
            'work_mem': '64MB',              # для COUNT(DISTINCT) / hash aggregate
            'enable_hashjoin': 'off',              
            'enable_mergejoin': 'off',             
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
        )
        SELECT $5::int - (
            SELECT count(*) FROM api_requests
            WHERE consumer = $1
              AND requested_at >= date_trunc('day', now())
              AND requested_at <  date_trunc('day', now()) + interval '1 day'
        ) AS left
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
            count = await search_count(conn, body.name, body.inn)
            rows = await last_cases(conn, body.name, body.inn, limit=5)
            left_after = await record_ok(conn, consumer, 'search', ip, body_dict)
        result = {'count': count, 'rows': rows}
        #result = {'count': count}
    finally:
        SEM.release()

    log.info(json.dumps({
        'event': 'search_ok', 'consumer': consumer,
        'ip': ip, 'quota_left': left_after,
    }))
    return JSONResponse(
        jsonable_encoder({'ok': True, 'quota_left': left_after, 'result': result}),
        headers={'X-Quota-Remaining': str(left_after)},
    )
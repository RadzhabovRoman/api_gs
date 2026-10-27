"""
selects.py — SQL-запросы и обёртки над asyncpg для API.

ВАЖНО: значения inn / name / limit подставляются в SQL ЛИТЕРАЛАМИ (с экранированием),
а не через $1/$2. Так планировщик Postgres видит константы и строит тот же план,
что и в pgAdmin (без generic-плана prepared statement).
"""
from typing import Optional


def _lit(value: str) -> str:
    """Безопасный SQL-литерал строки (аналог quote_literal)."""
    value = str(value)
    if "\x00" in value:
        raise ValueError("NUL character is not allowed in SQL literal")
    # standard_conforming_strings = on (по умолчанию) -> достаточно удвоить апострофы
    return "'" + value.replace("'", "''") + "'"


# ---------------------------------------------------------------------------
# COUNT: три ветки, как в pgAdmin; суммируются в Python
# ---------------------------------------------------------------------------
_COUNT_BY_INN = """
SELECT COUNT(DISTINCT pt.case_id)
FROM observe_user_cases_parts AS pt
WHERE pt.inn = {inn}
"""

_COUNT_BY_NAME_PT = """
SELECT COUNT(DISTINCT pt.case_id)
FROM observe_user_cases_parts AS pt
WHERE (pt.inn IS NULL OR pt.inn = '')
  AND to_tsvector('simple', extract_part_name(pt.part_name)) @@ plainto_tsquery('simple', norm_party({name}))
"""

_COUNT_BY_NAME_SR = """
SELECT COUNT(DISTINCT sr.id)
FROM observe_user_scrap_results AS sr
WHERE sr.has_case_status_3 = false
  AND (
      to_tsvector('simple', extract_plaintiff(sr.case_info)) @@ plainto_tsquery('simple', norm_party({name}))
      OR to_tsvector('simple', extract_defendant(sr.case_info)) @@ plainto_tsquery('simple', norm_party({name}))
  )
"""


# --- Count для случая, когда передан регион ({region} — целое число) ---
# Как в присланном SQL с раскомментированными JOIN: дела status_id = 3 и суды нужного региона.
_COUNT_BY_INN_REGION = """
SELECT COUNT(DISTINCT cs.id)
FROM observe_user_cases_parts AS pt
JOIN observe_user_cases AS cs ON cs.id = pt.case_id AND cs.status_id = 3
JOIN observe_user_scrap_results AS sr ON sr.id = cs.result_id
JOIN observe_user_scrap AS sc ON sc.id = sr.scrap_id
JOIN observe_user_ethelongloballink AS et ON et.id = sc.ethelon_id
JOIN observe_user_court AS ct ON ct.id = et.court_id AND ct.region_id = {region}
WHERE pt.inn = {inn}
"""

_COUNT_BY_NAME_PT_REGION = """
SELECT COUNT(DISTINCT cs.id)
FROM observe_user_cases_parts AS pt
JOIN observe_user_cases AS cs ON cs.id = pt.case_id AND cs.status_id = 3
JOIN observe_user_scrap_results AS sr ON sr.id = cs.result_id
JOIN observe_user_scrap AS sc ON sc.id = sr.scrap_id
JOIN observe_user_ethelongloballink AS et ON et.id = sc.ethelon_id
JOIN observe_user_court AS ct ON ct.id = et.court_id AND ct.region_id = {region}
WHERE (pt.inn IS NULL OR pt.inn = '')
  AND to_tsvector('simple', extract_part_name(pt.part_name)) @@ plainto_tsquery('simple', norm_party({name}))
"""

_COUNT_BY_NAME_SR_REGION = """
SELECT COUNT(DISTINCT sr.id)
FROM observe_user_scrap_results AS sr
JOIN observe_user_scrap AS sc ON sc.id = sr.scrap_id
JOIN observe_user_ethelongloballink AS et ON et.id = sc.ethelon_id
JOIN observe_user_court AS ct ON ct.id = et.court_id AND ct.region_id = {region}
WHERE sr.has_case_status_3 = false
  AND (
      to_tsvector('simple', extract_plaintiff(sr.case_info)) @@ plainto_tsquery('simple', norm_party({name}))
      OR to_tsvector('simple', extract_defendant(sr.case_info)) @@ plainto_tsquery('simple', norm_party({name}))
  )
"""

# Региональный count с JOIN тяжёлый (у крупных контрагентов > 30 с на холодном кэше),
# поэтому для него свой таймаут вместо command_timeout=30 пула.
COUNT_REGION_TIMEOUT = 120.0


def build_count_sql(name: str, inn: Optional[str], region_id: Optional[int] = None) -> str:
    """Тот же UNION ALL, что выполняется в pgAdmin (значения — литералы)."""
    n = _lit(name)
    parts = []
    if region_id is not None:
        r = int(region_id)
        if inn:
            parts.append(_COUNT_BY_INN_REGION.format(inn=_lit(inn), region=r))
        parts.append(_COUNT_BY_NAME_PT_REGION.format(name=n, region=r))
        parts.append(_COUNT_BY_NAME_SR_REGION.format(name=n, region=r))
    else:
        if inn:
            parts.append(_COUNT_BY_INN.format(inn=_lit(inn)))
        parts.append(_COUNT_BY_NAME_PT.format(name=n))
        parts.append(_COUNT_BY_NAME_SR.format(name=n))
    return "\nUNION ALL\n".join(parts)


# Порядок детерминирован: внутри каждой ветки ORDER BY date_in DESC, id DESC, чтобы при одинаковой
# date_in (у крупных контрагентов за день приходят сотни дел) выбирались одни и те же строки.
# Для ранней остановки по LIMIT нужен индекс:
#   CREATE INDEX sr_date_id_idx ON observe_user_scrap_results (date_in DESC, id DESC) INCLUDE (scrap_id);
# ---------------------------------------------------------------------------
# LAST CASES: каждая ветка — отдельный подзапрос с ORDER BY ... LIMIT
# ---------------------------------------------------------------------------

# Ветка 1: точный INN
_LAST_CASES_BY_INN = """
SELECT sr.*
FROM observe_user_scrap_results AS sr
JOIN observe_user_cases AS cs ON cs.result_id = sr.id AND cs.status_id = 3
JOIN observe_user_cases_parts AS pt ON pt.case_id = cs.id
    AND pt.inn = {inn}
ORDER BY sr.date_in DESC, sr.id DESC
LIMIT {limit}
"""

# Ветка 2: имя в частях дела (без INN)
_LAST_CASES_BY_NAME_PT = """
SELECT sr.*
FROM observe_user_scrap_results AS sr
JOIN observe_user_cases AS cs ON cs.result_id = sr.id AND cs.status_id = 3
JOIN observe_user_cases_parts AS pt ON pt.case_id = cs.id
    AND (pt.inn IS NULL OR pt.inn = '')
    AND to_tsvector('simple', extract_part_name(pt.part_name)) @@ plainto_tsquery('simple', norm_party({name}))
ORDER BY sr.date_in DESC, sr.id DESC
LIMIT {limit}
"""

# Ветка 3: имя в plaintiff/defendant
_LAST_CASES_BY_NAME_SR = """
SELECT sr.*
FROM observe_user_scrap_results AS sr
WHERE sr.has_case_status_3 = false
  AND (
      to_tsvector('simple', extract_plaintiff(sr.case_info)) @@ plainto_tsquery('simple', norm_party({name}))
      OR to_tsvector('simple', extract_defendant(sr.case_info)) @@ plainto_tsquery('simple', norm_party({name}))
  )
ORDER BY sr.date_in DESC, sr.id DESC
LIMIT {limit}
"""


# --- Аналогичные шаблоны для случая, когда передан регион ({region} — целое число) ---
# Суд/регион фильтруются внутри каждой ветки, ethelon_id берётся из observe_user_scrap.
_LAST_CASES_BY_INN_REGION = """
SELECT sc.ethelon_id, sr.*
FROM observe_user_scrap_results AS sr
JOIN observe_user_scrap AS sc ON sc.id = sr.scrap_id
JOIN observe_user_ethelongloballink AS et ON et.id = sc.ethelon_id
JOIN observe_user_court AS ct ON ct.id = et.court_id AND ct.region_id = {region}
JOIN observe_user_cases AS cs ON cs.result_id = sr.id AND cs.status_id = 3
JOIN observe_user_cases_parts AS pt ON pt.case_id = cs.id
    AND pt.inn = {inn}
ORDER BY sr.date_in DESC, sr.id DESC
LIMIT {limit}
"""

_LAST_CASES_BY_NAME_PT_REGION = """
SELECT sc.ethelon_id, sr.*
FROM observe_user_scrap_results AS sr
JOIN observe_user_scrap AS sc ON sc.id = sr.scrap_id
JOIN observe_user_ethelongloballink AS et ON et.id = sc.ethelon_id
JOIN observe_user_court AS ct ON ct.id = et.court_id AND ct.region_id = {region}
JOIN observe_user_cases AS cs ON cs.result_id = sr.id AND cs.status_id = 3
JOIN observe_user_cases_parts AS pt ON pt.case_id = cs.id
    AND (pt.inn IS NULL OR pt.inn = '')
    AND to_tsvector('simple', extract_part_name(pt.part_name)) @@ plainto_tsquery('simple', norm_party({name}))
ORDER BY sr.date_in DESC, sr.id DESC
LIMIT {limit}
"""

_LAST_CASES_BY_NAME_SR_REGION = """
SELECT sc.ethelon_id, sr.*
FROM observe_user_scrap_results AS sr
JOIN observe_user_scrap AS sc ON sc.id = sr.scrap_id
JOIN observe_user_ethelongloballink AS et ON et.id = sc.ethelon_id
JOIN observe_user_court AS ct ON ct.id = et.court_id AND ct.region_id = {region}
WHERE sr.has_case_status_3 = false
  AND (
      to_tsvector('simple', extract_plaintiff(sr.case_info)) @@ plainto_tsquery('simple', norm_party({name}))
      OR to_tsvector('simple', extract_defendant(sr.case_info)) @@ plainto_tsquery('simple', norm_party({name}))
  )
ORDER BY sr.date_in DESC, sr.id DESC
LIMIT {limit}
"""


async def search_count(conn, name: str, inn: Optional[str], region_id: Optional[int] = None) -> int:
    """Сумма COUNT'ов: один запрос UNION ALL, строки суммируются в Python."""
    timeout = COUNT_REGION_TIMEOUT if region_id is not None else None   # None = command_timeout пула
    rows = await conn.fetch(build_count_sql(name, inn, region_id), timeout=timeout)
    return sum(r[0] for r in rows)


# --- last_cases без региона: оптимизированная версия ---
# Как и в региональной: ветки отбирают только id и date_in (узкие колонки), полные колонки
# (case_url, case_info, judge) подтягиваются только для итоговых строк. Так не читаются
# широкие строки observe_user_scrap_results для тысяч дел, которые потом отбрасываются.
_TOP_BY_INN = """
SELECT sr.id, sr.date_in
FROM observe_user_scrap_results AS sr
JOIN observe_user_cases AS cs ON cs.result_id = sr.id AND cs.status_id = 3
JOIN observe_user_cases_parts AS pt ON pt.case_id = cs.id
    AND pt.inn = {inn}
ORDER BY sr.date_in DESC, sr.id DESC
LIMIT {limit}
"""

_TOP_BY_NAME_PT = """
SELECT sr.id, sr.date_in
FROM observe_user_scrap_results AS sr
JOIN observe_user_cases AS cs ON cs.result_id = sr.id AND cs.status_id = 3
JOIN observe_user_cases_parts AS pt ON pt.case_id = cs.id
    AND (pt.inn IS NULL OR pt.inn = '')
    AND to_tsvector('simple', extract_part_name(pt.part_name)) @@ plainto_tsquery('simple', norm_party({name}))
ORDER BY sr.date_in DESC, sr.id DESC
LIMIT {limit}
"""

_TOP_BY_NAME_SR = """
SELECT sr.id, sr.date_in
FROM observe_user_scrap_results AS sr
WHERE sr.has_case_status_3 = false
  AND (
      to_tsvector('simple', extract_plaintiff(sr.case_info)) @@ plainto_tsquery('simple', norm_party({name}))
      OR to_tsvector('simple', extract_defendant(sr.case_info)) @@ plainto_tsquery('simple', norm_party({name}))
  )
ORDER BY sr.date_in DESC, sr.id DESC
LIMIT {limit}
"""


def _build_last_cases_narrow_sql(n: str, i: Optional[str], lim: int) -> str:
    """last_cases без региона (n, i — готовые SQL-литералы), узкие ветки + подтягивание строк."""
    if i is not None:
        branches = f"""
                (
                    {_TOP_BY_INN.format(inn=i, limit=lim)}
                )
                UNION ALL
                (
                    {_TOP_BY_NAME_PT.format(name=n, limit=lim)}
                )
                UNION ALL
                (
                    {_TOP_BY_NAME_SR.format(name=n, limit=lim)}
                )
        """
    else:
        branches = f"""
                (
                    {_TOP_BY_NAME_PT.format(name=n, limit=lim)}
                )
                UNION ALL
                (
                    {_TOP_BY_NAME_SR.format(name=n, limit=lim)}
                )
        """

    return f"""
        SELECT sc.ethelon_id, sr.case_url, sr.case_num, sr.date_in, sr.case_info, sr.judge
        FROM (
            {branches}
        ) AS top
        JOIN observe_user_scrap_results AS sr ON sr.id = top.id
        JOIN observe_user_scrap AS sc ON sc.id = sr.scrap_id
        ORDER BY top.date_in DESC, top.id DESC
        LIMIT {lim}
    """


def build_last_cases_sql(name: str, inn: Optional[str], limit: int = 5,
                         region_id: Optional[int] = None) -> str:
    """
    Собирает итоговый SQL для last_cases (значения — литералы).
    Используется и в last_cases, и в debug-эндпоинтах (EXPLAIN),
    чтобы тестировался ровно тот же запрос.
    """
    lim = int(limit)
    n = _lit(name)
    i = _lit(inn) if inn else None
    if region_id is not None:
        return _build_last_cases_region_sql(n, i, lim, int(region_id))
    return _build_last_cases_narrow_sql(n, i, lim)


def _build_last_cases_plain_sql(n: str, i: Optional[str], lim: int) -> str:
    """
    Прежняя сборка last_cases без региона (SELECT sr.* в ветках, как в pgAdmin).
    Оставлена для сравнения результатов; в работе не используется.
    """
    if i is not None:
        branches = f"""
                (
                    {_LAST_CASES_BY_INN.format(inn=i, limit=lim)}
                )
                UNION ALL
                (
                    {_LAST_CASES_BY_NAME_PT.format(name=n, limit=lim)}
                )
                UNION ALL
                (
                    {_LAST_CASES_BY_NAME_SR.format(name=n, limit=lim)}
                )
        """
    else:
        branches = f"""
                (
                    {_LAST_CASES_BY_NAME_PT.format(name=n, limit=lim)}
                )
                UNION ALL
                (
                    {_LAST_CASES_BY_NAME_SR.format(name=n, limit=lim)}
                )
        """

    return f"""
        SELECT sc.ethelon_id, sr.case_url, sr.case_num, sr.date_in, sr.case_info, sr.judge
        FROM (
            {branches}
        ) AS sr
        JOIN observe_user_scrap AS sc ON sc.id = sr.scrap_id
        ORDER BY sr.date_in DESC, sr.id DESC
        LIMIT {lim}
    """


def _build_last_cases_region_sql_legacy(n: str, i: Optional[str], lim: int, region: int) -> str:
    """
    Прежняя версия last_cases с регионом (дословно как в pgAdmin, SELECT sr.* в ветках).
    Оставлена только для сравнения результатов; в работе не используется.
    """
    if i is not None:
        branches = f"""
                (
                    {_LAST_CASES_BY_INN_REGION.format(inn=i, region=region, limit=lim)}
                )
                UNION ALL
                (
                    {_LAST_CASES_BY_NAME_PT_REGION.format(name=n, region=region, limit=lim)}
                )
                UNION ALL
                (
                    {_LAST_CASES_BY_NAME_SR_REGION.format(name=n, region=region, limit=lim)}
                )
        """
    else:
        branches = f"""
                (
                    {_LAST_CASES_BY_NAME_PT_REGION.format(name=n, region=region, limit=lim)}
                )
                UNION ALL
                (
                    {_LAST_CASES_BY_NAME_SR_REGION.format(name=n, region=region, limit=lim)}
                )
        """

    return f"""
        SELECT sr.ethelon_id, sr.case_url, sr.case_num, sr.date_in, sr.case_info, sr.judge
        FROM (
            {branches}
        ) AS sr
        ORDER BY sr.date_in DESC, sr.id DESC
        LIMIT {lim}
    """


# --- Региональный last_cases: оптимизированная версия ---
# Ветки отбирают только id и date_in (узкие колонки), поэтому обход по дате идёт
# по индексу sr_date_inc_idx (date_in DESC) INCLUDE (id, scrap_id) без обращения к таблице.
# Полные колонки (case_url, case_info, judge) подтягиваются только для итоговых строк.
# Регион:
#   - ветки по имени (2 и 3): sc.ethelon_id = ANY(массив судов региона) — массив считается один раз,
#     вместо соединения с Join Filter на миллионы сравнений (ветка 2 быстрее в 2-3 раза);
#   - ветка по ИНН (1): обычное соединение с судами. С ANY планировщик начинает с индекса по ИНН,
#     собирает все дела контрагента и работает в 3-4 раза медленнее, чем при обходе по дате.
_TOP_BY_INN_REGION = """
SELECT sr.id, sr.date_in
FROM observe_user_scrap_results AS sr
JOIN observe_user_scrap AS sc ON sc.id = sr.scrap_id
JOIN observe_user_ethelongloballink AS et ON et.id = sc.ethelon_id
JOIN observe_user_court AS ct ON ct.id = et.court_id AND ct.region_id = {region}
JOIN observe_user_cases AS cs ON cs.result_id = sr.id AND cs.status_id = 3
JOIN observe_user_cases_parts AS pt ON pt.case_id = cs.id
    AND pt.inn = {inn}
ORDER BY sr.date_in DESC, sr.id DESC
LIMIT {limit}
"""

_TOP_BY_NAME_PT_REGION = """
SELECT sr.id, sr.date_in
FROM observe_user_scrap_results AS sr
JOIN observe_user_scrap AS sc ON sc.id = sr.scrap_id
JOIN observe_user_cases AS cs ON cs.result_id = sr.id AND cs.status_id = 3
JOIN observe_user_cases_parts AS pt ON pt.case_id = cs.id
    AND (pt.inn IS NULL OR pt.inn = '')
    AND to_tsvector('simple', extract_part_name(pt.part_name)) @@ plainto_tsquery('simple', norm_party({name}))
WHERE sc.ethelon_id = ANY (ARRAY(
    SELECT et.id
    FROM observe_user_ethelongloballink AS et
    JOIN observe_user_court AS ct ON ct.id = et.court_id AND ct.region_id = {region}
))
ORDER BY sr.date_in DESC, sr.id DESC
LIMIT {limit}
"""

_TOP_BY_NAME_SR_REGION = """
SELECT sr.id, sr.date_in
FROM observe_user_scrap_results AS sr
JOIN observe_user_scrap AS sc ON sc.id = sr.scrap_id
WHERE sc.ethelon_id = ANY (ARRAY(
    SELECT et.id
    FROM observe_user_ethelongloballink AS et
    JOIN observe_user_court AS ct ON ct.id = et.court_id AND ct.region_id = {region}
))
  AND sr.has_case_status_3 = false
  AND (
      to_tsvector('simple', extract_plaintiff(sr.case_info)) @@ plainto_tsquery('simple', norm_party({name}))
      OR to_tsvector('simple', extract_defendant(sr.case_info)) @@ plainto_tsquery('simple', norm_party({name}))
  )
ORDER BY sr.date_in DESC, sr.id DESC
LIMIT {limit}
"""


def _build_last_cases_region_sql(n: str, i: Optional[str], lim: int, region: int) -> str:
    """
    last_cases с фильтром по региону (n, i — уже готовые SQL-литералы).
    Результат тот же, что у прежней версии, но быстрее (см. комментарий выше).
    """
    if i is not None:
        branches = f"""
                (
                    {_TOP_BY_INN_REGION.format(inn=i, region=region, limit=lim)}
                )
                UNION ALL
                (
                    {_TOP_BY_NAME_PT_REGION.format(name=n, region=region, limit=lim)}
                )
                UNION ALL
                (
                    {_TOP_BY_NAME_SR_REGION.format(name=n, region=region, limit=lim)}
                )
        """
    else:
        branches = f"""
                (
                    {_TOP_BY_NAME_PT_REGION.format(name=n, region=region, limit=lim)}
                )
                UNION ALL
                (
                    {_TOP_BY_NAME_SR_REGION.format(name=n, region=region, limit=lim)}
                )
        """

    return f"""
        SELECT sc.ethelon_id, sr.case_url, sr.case_num, sr.date_in, sr.case_info, sr.judge
        FROM (
            {branches}
        ) AS top
        JOIN observe_user_scrap_results AS sr ON sr.id = top.id
        JOIN observe_user_scrap AS sc ON sc.id = sr.scrap_id
        ORDER BY top.date_in DESC, top.id DESC
        LIMIT {lim}
    """


def build_last_cases_sql_legacy(name: str, inn: Optional[str], limit: int = 5,
                                region_id: Optional[int] = None) -> str:
    """Прежний SQL (до оптимизации). Нужен только для сравнения результатов A/B."""
    if region_id is None:
        return _build_last_cases_plain_sql(_lit(name), _lit(inn) if inn else None, int(limit))
    return _build_last_cases_region_sql_legacy(
        _lit(name), _lit(inn) if inn else None, int(limit), int(region_id))


async def last_cases(conn, name: str, inn: Optional[str], limit: int = 5,
                     region_id: Optional[int] = None) -> list:
    """До `limit` самых свежих кейсов (тот же запрос, что в pgAdmin)."""
    rows = await conn.fetch(build_last_cases_sql(name, inn, limit, region_id))
    return [dict(r) for r in rows]


async def explain_last_cases(conn, name: str, inn: Optional[str], limit: int = 5,
                             region_id: Optional[int] = None) -> str:
    """EXPLAIN (ANALYZE, BUFFERS) для last_cases — для debug-эндпоинта."""
    sql = "EXPLAIN (ANALYZE, BUFFERS) " + build_last_cases_sql(name, inn, limit, region_id)
    rows = await conn.fetch(sql)
    return "\n".join(r[0] for r in rows)


async def explain_count(conn, name: str, inn: Optional[str], region_id: Optional[int] = None) -> dict:
    """EXPLAIN (ANALYZE, BUFFERS) того же UNION ALL, что выполняет search_count."""
    timeout = COUNT_REGION_TIMEOUT if region_id is not None else None
    rows = await conn.fetch("EXPLAIN (ANALYZE, BUFFERS) " + build_count_sql(name, inn, region_id),
                            timeout=timeout)
    return {'count_union_all': "\n".join(r[0] for r in rows)}

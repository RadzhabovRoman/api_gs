"""
last_days_selects.py — SQL-запросы и обёртки над asyncpg для API (last_5_days).

ВАЖНО: значения inn / name / regions / window подставляются в SQL ЛИТЕРАЛАМИ
(с экранированием или приведением к int), а не через $1/$2. Так планировщик Postgres
видит константы и строит тот же план, что и в pgAdmin (без generic-плана prepared statement).

Состав:
  - last_days: до LAST_DAYS_MAX_LIMIT самых свежих дел за последние LAST_DAYS_WINDOW_DAYS
    дней по списку регионов; ветки выбирают только id и date_in (узкие колонки),
    полные строки подтягиваются для итоговых строк.

sr.date_in — тип date, поэтому фильтр по свежести сравнивает date с date
(current_date - N), без now()/timestamptz: так используется индекс по date_in
и нет сюрпризов на границе дня.

Нужные индексы (те же, что для last_cases):
  CREATE INDEX sr_date_id_idx ON observe_user_scrap_results (date_in DESC, id DESC) INCLUDE (scrap_id);
  CREATE INDEX sr_id_scrap_date_idx ON observe_user_scrap_results (id) INCLUDE (scrap_id, date_in);
"""
from typing import Optional


# Верхняя граница выборки: больше 1000 дел на одно лицо быть не может.
LAST_DAYS_MAX_LIMIT = 1000

# Окно «свежести»: последние N календарных дней.
# sr.date_in — date, поэтому сравниваем с current_date - N (date - int = date).
LAST_DAYS_WINDOW_DAYS = 5


def _lit(value: str) -> str:
    """Безопасный SQL-литерал строки (аналог quote_literal)."""
    value = str(value)
    if "\x00" in value:
        raise ValueError("NUL character is not allowed in SQL literal")
    # standard_conforming_strings = on (по умолчанию) -> достаточно удвоить апострофы
    return "'" + value.replace("'", "''") + "'"


def _regions_sql(region_id_lst: list) -> str:
    """
    SQL-список регионов для IN (...): "1, 2, 3".
    Значения приводятся к int (защита от инъекций), пустой список запрещён.
    """
    if not region_id_lst:
        raise ValueError('region_id_lst must not be empty')
    return ', '.join(str(int(r)) for r in region_id_lst)


# ---------------------------------------------------------------------------
# LAST DAYS: те же три ветки, что в last_cases, но:
#   - регион задан списком (IN (...));
#   - добавлен фильтр sr.date_in за последние LAST_DAYS_WINDOW_DAYS дней;
#   - count не нужен;
#   - LIMIT — фиксированный LAST_DAYS_MAX_LIMIT.
# ---------------------------------------------------------------------------
# Порядок детерминирован: внутри каждой ветки ORDER BY date_in DESC, id DESC.
# Для ранней остановки по LIMIT нужен индекс sr_date_id_idx (см. начало файла).

_TOP_BY_INN_REGION = """
SELECT sr.id, sr.date_in
FROM observe_user_scrap_results AS sr
JOIN observe_user_scrap AS sc ON sc.id = sr.scrap_id
JOIN observe_user_ethelongloballink AS et ON et.id = sc.ethelon_id
JOIN observe_user_court AS ct ON ct.id = et.court_id AND ct.region_id IN ({regions})
JOIN observe_user_cases AS cs ON cs.result_id = sr.id AND cs.status_id = 3
JOIN observe_user_cases_parts AS pt ON pt.case_id = cs.id
    AND pt.inn = {inn}
WHERE sr.date_in >= current_date - {window_days}
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
    JOIN observe_user_court AS ct ON ct.id = et.court_id AND ct.region_id IN ({regions})
))
  AND sr.date_in >= current_date - {window_days}
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
    JOIN observe_user_court AS ct ON ct.id = et.court_id AND ct.region_id IN ({regions})
))
  AND sr.date_in >= current_date - {window_days}
  AND sr.has_case_status_3 = false
  AND (
      to_tsvector('simple', extract_plaintiff(sr.case_info)) @@ plainto_tsquery('simple', norm_party({name}))
      OR to_tsvector('simple', extract_defendant(sr.case_info)) @@ plainto_tsquery('simple', norm_party({name}))
  )
ORDER BY sr.date_in DESC, sr.id DESC
LIMIT {limit}
"""


def _build_last_days_sql(n: str, i: Optional[str], regions: str) -> str:
    """
    last_days (n, i — готовые SQL-литералы; regions — "1, 2, 3").
    LIMIT всегда LAST_DAYS_MAX_LIMIT, окно — LAST_DAYS_WINDOW_DAYS дней.
    """
    lim = LAST_DAYS_MAX_LIMIT
    wd = LAST_DAYS_WINDOW_DAYS

    if i is not None:
        branches = f"""
                (
                    {_TOP_BY_INN_REGION.format(inn=i, regions=regions, window_days=wd, limit=lim)}
                )
                UNION ALL
                (
                    {_TOP_BY_NAME_PT_REGION.format(name=n, regions=regions, window_days=wd, limit=lim)}
                )
                UNION ALL
                (
                    {_TOP_BY_NAME_SR_REGION.format(name=n, regions=regions, window_days=wd, limit=lim)}
                )
        """
    else:
        branches = f"""
                (
                    {_TOP_BY_NAME_PT_REGION.format(name=n, regions=regions, window_days=wd, limit=lim)}
                )
                UNION ALL
                (
                    {_TOP_BY_NAME_SR_REGION.format(name=n, regions=regions, window_days=wd, limit=lim)}
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


def build_last_days_sql(name: str, inn: Optional[str], region_id_lst: list) -> str:
    """Собирает итоговый SQL для last_days (значения — литералы)."""
    n = _lit(name)
    i = _lit(inn) if inn else None
    regions = _regions_sql(region_id_lst)
    return _build_last_days_sql(n, i, regions)


async def last_days(conn, name: str, inn: Optional[str], region_id_lst: list) -> list:
    """
    До LAST_DAYS_MAX_LIMIT самых свежих кейсов за последние LAST_DAYS_WINDOW_DAYS дней
    по списку регионов.
    """
    rows = await conn.fetch(build_last_days_sql(name, inn, region_id_lst))
    return [dict(r) for r in rows]
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


def build_count_sql(name: str, inn: Optional[str]) -> str:
    """Тот же UNION ALL, что выполняется в pgAdmin (значения — литералы)."""
    n = _lit(name)
    parts = []
    if inn:
        parts.append(_COUNT_BY_INN.format(inn=_lit(inn)))
    parts.append(_COUNT_BY_NAME_PT.format(name=n))
    parts.append(_COUNT_BY_NAME_SR.format(name=n))
    return "\nUNION ALL\n".join(parts)


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
ORDER BY sr.date_in DESC
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
ORDER BY sr.date_in DESC
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
ORDER BY sr.date_in DESC
LIMIT {limit}
"""


async def search_count(conn, name: str, inn: Optional[str]) -> int:
    """Сумма COUNT'ов: один запрос UNION ALL, строки суммируются в Python."""
    rows = await conn.fetch(build_count_sql(name, inn))
    return sum(r[0] for r in rows)


def build_last_cases_sql(name: str, inn: Optional[str], limit: int = 5) -> str:
    """
    Собирает итоговый SQL для last_cases (значения — литералы).
    Используется и в last_cases, и в debug-эндпоинтах (EXPLAIN),
    чтобы тестировался ровно тот же запрос.
    """
    lim = int(limit)
    n = _lit(name)

    if inn:
        i = _lit(inn)
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


async def last_cases(conn, name: str, inn: Optional[str], limit: int = 5) -> list:
    """До `limit` самых свежих кейсов (тот же запрос, что в pgAdmin)."""
    rows = await conn.fetch(build_last_cases_sql(name, inn, limit))
    return [dict(r) for r in rows]


async def explain_last_cases(conn, name: str, inn: Optional[str], limit: int = 5) -> str:
    """EXPLAIN (ANALYZE, BUFFERS) для last_cases — для debug-эндпоинта."""
    sql = "EXPLAIN (ANALYZE, BUFFERS) " + build_last_cases_sql(name, inn, limit)
    rows = await conn.fetch(sql)
    return "\n".join(r[0] for r in rows)


async def explain_count(conn, name: str, inn: Optional[str]) -> dict:
    """EXPLAIN (ANALYZE, BUFFERS) того же UNION ALL, что выполняет search_count."""
    rows = await conn.fetch("EXPLAIN (ANALYZE, BUFFERS) " + build_count_sql(name, inn))
    return {'count_union_all': "\n".join(r[0] for r in rows)}
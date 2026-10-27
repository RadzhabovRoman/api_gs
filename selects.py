"""
selects.py — SQL-запросы и обёртки над asyncpg для API.
"""
from typing import Optional


# ---------------------------------------------------------------------------
# COUNT: сколько всего дел по name + inn
# ---------------------------------------------------------------------------
_COUNT_SQL_BY_INN = """
    SELECT COUNT(DISTINCT pt.case_id)
    FROM observe_user_cases_parts AS pt
    WHERE pt.inn = $1
"""

_COUNT_SQL_BY_NAME = """
    SELECT
      (SELECT COUNT(DISTINCT pt.case_id)
       FROM observe_user_cases_parts AS pt
       WHERE (pt.inn IS NULL OR pt.inn = '')
         AND to_tsvector('simple', extract_part_name(pt.part_name))
             @@ plainto_tsquery('simple', norm_party($1))
      )
      +
      (SELECT COUNT(DISTINCT sr.id)
       FROM observe_user_scrap_results AS sr
       WHERE sr.has_case_status_3 = false
         AND (
           to_tsvector('simple', extract_plaintiff(sr.case_info))
               @@ plainto_tsquery('simple', norm_party($1))
           OR to_tsvector('simple', extract_defendant(sr.case_info))
               @@ plainto_tsquery('simple', norm_party($1))
         )
      )
"""


# ---------------------------------------------------------------------------
# LAST CASES: 5 самых свежих кейсов.
#
# Вариант с inn:  $1 = inn, $2 = limit, $3 = name
#   ветка 1 (по inn):        $1 (inn),   $2 (limit)
#   ветки 2-3 (по name):     $3 (name),  $2 (limit)
#
# Вариант без inn: $1 = name, $2 = limit
#   обе ветки:               $1 (name),  $2 (limit)
# ---------------------------------------------------------------------------

# --- с inn ---
_LAST_CASES_BY_INN = """
    (
        SELECT sr.*
        FROM observe_user_scrap_results AS sr
        JOIN observe_user_cases AS cs
          ON cs.result_id = sr.id AND cs.status_id = 3
        JOIN observe_user_cases_parts AS pt
          ON pt.case_id = cs.id AND pt.inn = $1
        ORDER BY sr.date_in DESC NULLS LAST, sr.id DESC
        LIMIT $2
    )
"""

_LAST_CASES_BY_NAME_PT = """
    (
        SELECT sr.*
        FROM observe_user_scrap_results AS sr
        JOIN observe_user_cases AS cs
          ON cs.result_id = sr.id AND cs.status_id = 3
        JOIN observe_user_cases_parts AS pt
          ON pt.case_id = cs.id
         AND (pt.inn IS NULL OR pt.inn = '')
         AND to_tsvector('simple', extract_part_name(pt.part_name))
             @@ plainto_tsquery('simple', norm_party($3))
        ORDER BY sr.date_in DESC NULLS LAST, sr.id DESC
        LIMIT $2
    )
"""

_LAST_CASES_BY_NAME_SR = """
    (
        SELECT sr.*
        FROM observe_user_scrap_results AS sr
        WHERE sr.has_case_status_3 = false
          AND (
            to_tsvector('simple', extract_plaintiff(sr.case_info))
                @@ plainto_tsquery('simple', norm_party($3))
            OR to_tsvector('simple', extract_defendant(sr.case_info))
                @@ plainto_tsquery('simple', norm_party($3))
          )
        ORDER BY sr.date_in DESC NULLS LAST, sr.id DESC
        LIMIT $2
    )
"""

# --- без inn (те же ветки, но name = $1) ---
_LAST_CASES_BY_NAME_PT_NOINN = """
    (
        SELECT sr.*
        FROM observe_user_scrap_results AS sr
        JOIN observe_user_cases AS cs
          ON cs.result_id = sr.id AND cs.status_id = 3
        JOIN observe_user_cases_parts AS pt
          ON pt.case_id = cs.id
         AND (pt.inn IS NULL OR pt.inn = '')
         AND to_tsvector('simple', extract_part_name(pt.part_name))
             @@ plainto_tsquery('simple', norm_party($1))
        ORDER BY sr.date_in DESC NULLS LAST, sr.id DESC
        LIMIT $2
    )
"""

_LAST_CASES_BY_NAME_SR_NOINN = """
    (
        SELECT sr.*
        FROM observe_user_scrap_results AS sr
        WHERE sr.has_case_status_3 = false
          AND (
            to_tsvector('simple', extract_plaintiff(sr.case_info))
                @@ plainto_tsquery('simple', norm_party($1))
            OR to_tsvector('simple', extract_defendant(sr.case_info))
                @@ plainto_tsquery('simple', norm_party($1))
          )
        ORDER BY sr.date_in DESC NULLS LAST, sr.id DESC
        LIMIT $2
    )
"""


async def count_by_inn(conn, inn: str) -> int:
    """COUNT(DISTINCT pt.case_id) по точному ИНН. Вызывать только если inn непустой."""
    return await conn.fetchval(_COUNT_SQL_BY_INN, inn)


async def count_by_name(conn, name: str) -> int:
    """COUNT по part_name (inn пуст) + COUNT по plaintiff/defendant в case_info."""
    return await conn.fetchval(_COUNT_SQL_BY_NAME, name)


async def search_count(conn, name: str, inn: Optional[str]) -> int:
    """
    Сумма COUNT'ов: по ИНН (если есть) + по name (всегда).
    name — исходный; norm_party применяется в SQL.
    """
    total = 0
    if inn:
        total += await count_by_inn(conn, inn)
    total += await count_by_name(conn, name)
    return total


async def last_cases(conn, name: str, inn: Optional[str], limit: int = 5) -> list:
    """
    До `limit` самых свежих кейсов (date_in DESC, id DESC).
    Если inn передан — добавляется ветка по pt.inn.
    Значения передаются только через $N, f-string — для склейки констант.
    """
    if inn:
        sql = f"""
            SELECT sc.ethelon_id, sr.case_url, sr.case_num, sr.date_in,
                   sr.case_info, sr.judge
            FROM (
                {_LAST_CASES_BY_INN}
                UNION ALL
                {_LAST_CASES_BY_NAME_PT}
                UNION ALL
                {_LAST_CASES_BY_NAME_SR}
            ) AS sr
            JOIN observe_user_scrap sc ON sc.id = sr.scrap_id
            ORDER BY sr.date_in DESC NULLS LAST, sr.id DESC
            LIMIT $2
        """
        rows = await conn.fetch(sql, inn, limit, name)
    else:
        sql = f"""
            SELECT sc.ethelon_id, sr.case_url, sr.case_num, sr.date_in,
                   sr.case_info, sr.judge
            FROM (
                {_LAST_CASES_BY_NAME_PT_NOINN}
                UNION ALL
                {_LAST_CASES_BY_NAME_SR_NOINN}
            ) AS sr
            JOIN observe_user_scrap sc ON sc.id = sr.scrap_id
            ORDER BY sr.date_in DESC NULLS LAST, sr.id DESC
            LIMIT $2
        """
        rows = await conn.fetch(sql, name, limit)

    return [dict(r) for r in rows]
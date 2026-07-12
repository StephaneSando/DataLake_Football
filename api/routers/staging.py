from typing import Optional
import os

from fastapi import APIRouter, HTTPException, Query
import psycopg2
import psycopg2.extras

router = APIRouter()


def get_pg_connection():
    return psycopg2.connect(
        host=os.environ["PG_HOST"],
        database=os.environ["PG_DB"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )


@router.get("/", summary="Cleaned matches with form features")
def get_staging_matches(
    team: Optional[str] = Query(None),
    season: Optional[str] = Query(None),
    limit: int = Query(100, le=1000),
    offset: int = Query(0, ge=0),
):
    conn = get_pg_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        where_clauses, params = [], []
        if team:
            where_clauses.append("(home_team ILIKE %s OR away_team ILIKE %s)")
            params.extend([f"%{team}%", f"%{team}%"])
        if season:
            where_clauses.append("season = %s")
            params.append(season)

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        cur.execute(
            f"SELECT * FROM staging_matches {where_sql} ORDER BY date DESC LIMIT %s OFFSET %s",
            params + [limit, offset],
        )
        rows = cur.fetchall()
        return {
            "zone": "staging",
            "count": len(rows),
            "filters": {"team": team, "season": season},
            "matches": [dict(row) for row in rows],
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Staging error: {exc}")
    finally:
        cur.close()
        conn.close()

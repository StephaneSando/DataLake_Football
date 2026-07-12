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


@router.get("/", summary="XGBoost match result predictions")
def get_predictions(
    home_team: Optional[str] = Query(None),
    away_team: Optional[str] = Query(None),
    limit: int = Query(100, le=1000),
    offset: int = Query(0, ge=0),
):
    conn = get_pg_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        where_clauses, params = [], []
        if home_team:
            where_clauses.append("home_team ILIKE %s")
            params.append(f"%{home_team}%")
        if away_team:
            where_clauses.append("away_team ILIKE %s")
            params.append(f"%{away_team}%")
        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        cur.execute(
            f"SELECT * FROM curated_predictions {where_sql} ORDER BY date DESC LIMIT %s OFFSET %s",
            params + [limit, offset],
        )
        rows = cur.fetchall()

        cur.execute(
            """
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN actual_result = predicted_result THEN 1 ELSE 0 END) AS correct
            FROM curated_predictions
            WHERE actual_result IS NOT NULL
            """
        )
        stats = cur.fetchone()
        accuracy = round(stats["correct"] / stats["total"], 4) if stats["total"] else None
        return {
            "zone": "curated",
            "model": "XGBoost match outcome predictor (H/D/A)",
            "global_accuracy": accuracy,
            "count": len(rows),
            "predictions": [dict(row) for row in rows],
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Curated error: {exc}")
    finally:
        cur.close()
        conn.close()

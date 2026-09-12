"""Production readiness probe for the public API."""

from fastapi import APIRouter, HTTPException, status

from auth import RequireAPIKey
from database import create_connection

router = APIRouter()


@router.get(
    "/health",
    operation_id="get_health",
    tags=["Health"],
    summary="Check API and content database readiness",
    description=(
        "Returns `status: ok` only when the API can read at least one row from "
        "the languages table. Readiness probes are excluded from request "
        "statistics so monitoring does not inflate user traffic."
    ),
    responses={503: {"description": "API or content database is not ready"}},
)
def get_health(api_key: bool = RequireAPIKey) -> dict[str, str]:
    connection = None
    cursor = None
    try:
        connection = create_connection()
        if connection is None:
            raise RuntimeError("database connection unavailable")
        cursor = connection.cursor()
        cursor.execute("SELECT 1 FROM languages LIMIT 1")
        if cursor.fetchone() is None:
            raise RuntimeError("languages table is empty")
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service unavailable",
        )
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass

    return {"status": "ok"}

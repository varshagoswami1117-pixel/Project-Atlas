import hmac
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator

from dotenv import load_dotenv

# Explicitly find and load the .env file inside the backend folder before doing anything else
current_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(dotenv_path=os.path.join(current_dir, ".env"))

from fastapi import (
    Depends,
    FastAPI,
    File,
    Form,
    HTTPException,
    Request,
    Response,
    UploadFile,
)
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from starlette.responses import JSONResponse
from starlette.staticfiles import StaticFiles

from backend.database import (
    Client,
    ClientStatus,
    Report,
    dispose_db,
    get_db,
    get_readonly_db,
)
from backend.agents.workflow import WorkflowUnavailableError, run_report_copilot
from backend.ingestion import IngestionError, ingest_report
from backend.uploads import MAX_UPLOAD_BYTES, ingest_uploaded_report
from backend.web_auth import (
    WebSession,
    create_web_session,
    destroy_web_session,
    require_web_csrf,
    require_web_session,
)

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def require_api_key(api_key: str | None = Depends(api_key_header)) -> None:
    expected_api_key = os.getenv("ATLAS_API_KEY")
    if not expected_api_key:
        raise HTTPException(
            status_code=503,
            detail="API key authentication is not configured.",
        )
    if api_key is None or not hmac.compare_digest(api_key, expected_api_key):
        raise HTTPException(status_code=401, detail="Invalid or missing API key.")


class IngestRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    report_id: int = Field(gt=0)
    sector: str = Field(min_length=1, max_length=150)
    region: str = Field(min_length=1, max_length=150)
    year: int = Field(ge=1900, le=2200)
    source_path: str | None = Field(default=None, max_length=1024)

    @field_validator("sector", "region")
    @classmethod
    def validate_metadata_text(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("Metadata values cannot be blank.")
        return value

    @field_validator("source_path")
    @classmethod
    def normalize_source_path(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("source_path cannot be blank when provided.")
        return value


class ChatRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    client_id: int = Field(gt=0)
    query: str = Field(min_length=1, max_length=4000)

    @field_validator("query")
    @classmethod
    def validate_query(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("query cannot be blank.")
        return value


class WebSessionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    password: str = Field(min_length=1, max_length=512)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
    try:
        yield
    finally:
        dispose_db()


app = FastAPI(
    title="Project Atlas - Report Copilot",
    description="A secure API for report-based insights.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def secure_frontend_responses(request: Request, call_next):
    if request.url.path == "/ui/api/ingest":
        content_length = request.headers.get("content-length")
        if content_length is not None:
            try:
                request_size = int(content_length)
            except ValueError:
                return JSONResponse(
                    status_code=400,
                    content={"detail": "Invalid Content-Length header."},
                )
            if request_size > MAX_UPLOAD_BYTES + 1024 * 1024:
                return JSONResponse(
                    status_code=413,
                    content={"detail": "Upload request exceeds the allowed size."},
                )

    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "same-origin")
    response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
    if request.url.path.startswith("/ui/api/"):
        response.headers["Cache-Control"] = "no-store"
    if request.url.scheme == "https":
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000")
    if request.url.path == "/app" or request.url.path.startswith("/app/"):
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; "
            "connect-src 'self'; form-action 'self'; base-uri 'self'; frame-ancestors 'none'; "
            "object-src 'none'"
        )
    return response


@app.get("/")
def home():
    return {
        "message": "Project Atlas API is running!",
        "status": "success",
    }


@app.get("/health")
def health_check():
    return {"status": "healthy"}


@app.post("/ui/api/session")
def sign_in_to_web_app(
    payload: WebSessionRequest,
    request: Request,
    response: Response,
):
    return create_web_session(payload.password, request, response)


@app.get("/ui/api/session")
def get_current_web_session(
    response: Response,
    session: WebSession = Depends(require_web_session),
):
    response.headers["Cache-Control"] = "no-store"
    return {"csrf_token": session.csrf_token}


@app.delete("/ui/api/session")
def sign_out_of_web_app(
    request: Request,
    response: Response,
    _: WebSession = Depends(require_web_csrf),
):
    destroy_web_session(request, response)
    return {"status": "signed_out"}


@app.get("/ui/api/reports")
def list_registered_reports(
    _: WebSession = Depends(require_web_session),
    readonly_db: Session = Depends(get_readonly_db),
):
    statement = (
        select(
            Report.id,
            Report.title,
            Report.publisher,
            Report.publication_date,
            Report.ingestion_status,
        )
        .order_by(Report.id.desc())
        .limit(200)
    )
    try:
        rows = readonly_db.execute(statement).all()
    except SQLAlchemyError as error:
        raise HTTPException(
            status_code=503,
            detail="Could not load registered reports.",
        ) from error
    return {
        "reports": [
            {
                "id": row.id,
                "title": row.title,
                "publisher": row.publisher,
                "publication_date": (
                    row.publication_date.isoformat() if row.publication_date else None
                ),
                "ingestion_status": (
                    row.ingestion_status.value
                    if hasattr(row.ingestion_status, "value")
                    else row.ingestion_status
                ),
            }
            for row in rows
        ]
    }


def _execute_chat(payload: ChatRequest, db: Session, readonly_db: Session):
    client = db.get(Client, payload.client_id)
    if client is None:
        raise HTTPException(
            status_code=404,
            detail="Client ID is not registered. Enter a numeric ID from the clients table.",
        )
    if client.status != ClientStatus.ACTIVE:
        raise HTTPException(status_code=403, detail="Client account is not active.")
    response = run_report_copilot(db, readonly_db, payload.client_id, payload.query)
    db.commit()
    return response


def _chat_error_response(error: Exception) -> HTTPException:
    if isinstance(error, WorkflowUnavailableError):
        return HTTPException(
            status_code=503,
            detail="The report retrieval service is temporarily unavailable.",
        )
    if isinstance(error, SQLAlchemyError):
        return HTTPException(
            status_code=503,
            detail="The database service is temporarily unavailable.",
        )
    raise error


@app.post("/chat")
def chat(
    payload: ChatRequest,
    db: Session = Depends(get_db),
    readonly_db: Session = Depends(get_readonly_db),
    _: None = Depends(require_api_key),
):
    try:
        return _execute_chat(payload, db, readonly_db)
    except (WorkflowUnavailableError, SQLAlchemyError) as error:
        db.rollback()
        raise _chat_error_response(error) from error


@app.post("/ui/api/chat")
def web_chat(
    payload: ChatRequest,
    _: WebSession = Depends(require_web_csrf),
    db: Session = Depends(get_db),
    readonly_db: Session = Depends(get_readonly_db),
):
    try:
        return _execute_chat(payload, db, readonly_db)
    except (WorkflowUnavailableError, SQLAlchemyError) as error:
        db.rollback()
        raise _chat_error_response(error) from error


@app.post("/ingest")
def ingest_document(
    payload: IngestRequest,
    db: Session = Depends(get_db),
    _: None = Depends(require_api_key),
):
    try:
        result = ingest_report(
            db,
            report_id=payload.report_id,
            sector=payload.sector,
            region=payload.region,
            year=payload.year,
            source_path=payload.source_path,
        )
        return {"status": "success", **result}
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except IngestionError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except RuntimeError as error:
        raise HTTPException(status_code=500, detail=str(error)) from error


@app.post("/ui/api/ingest")
def ingest_uploaded_document(
    report_id: int = Form(gt=0),
    sector: str = Form(min_length=1, max_length=150),
    region: str = Form(min_length=1, max_length=150),
    year: int = Form(ge=1900, le=2200),
    file: UploadFile = File(...),
    _: WebSession = Depends(require_web_csrf),
    db: Session = Depends(get_db),
):
    cleaned_sector = sector.strip()
    cleaned_region = region.strip()
    if not cleaned_sector or not cleaned_region:
        raise HTTPException(status_code=422, detail="Sector and region cannot be blank.")

    try:
        content = file.file.read(MAX_UPLOAD_BYTES + 1)
        result = ingest_uploaded_report(
            db,
            report_id=report_id,
            sector=cleaned_sector,
            region=cleaned_region,
            year=year,
            filename=file.filename or "",
            content=content,
        )
        return {"status": "success", **result}
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except IngestionError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except SQLAlchemyError as error:
        db.rollback()
        raise HTTPException(
            status_code=503,
            detail="The database service is temporarily unavailable.",
        ) from error
    except RuntimeError as error:
        raise HTTPException(status_code=500, detail=str(error)) from error
    finally:
        file.file.close()


@app.get("/test-db")
def test_database_connection(db: Session = Depends(get_db)):
    """Tests the connection to the MySQL database and returns a success status."""
    try:
        mysql_version = db.execute(text("SELECT VERSION()")).scalar_one()
        return {
            "status": "success",
            "message": "Database connection verified successfully!",
            "mysql_version": mysql_version,
        }
    except SQLAlchemyError as error:
        raise HTTPException(
            status_code=500,
            detail="Database connection failed.",
        ) from error


app.mount(
    "/app",
    StaticFiles(directory=str(Path(__file__).resolve().parents[1] / "frontend"), html=True),
    name="frontend",
)
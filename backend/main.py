import hmac
import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from dotenv import load_dotenv

# Explicitly find and load the .env file inside the backend folder before doing anything else
current_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(dotenv_path=os.path.join(current_dir, ".env"))

from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from backend.database import (
    Client,
    ClientStatus,
    dispose_db,
    get_db,
    get_readonly_db,
)
from backend.ingestion import IngestionError, ingest_report
from backend.agents.workflow import WorkflowUnavailableError, run_report_copilot

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


@app.get("/")
def home():
    return {
        "message": "Project Atlas API is running!",
        "status": "success",
    }


@app.get("/health")
def health_check():
    return {"status": "healthy"}


@app.post("/chat")
def chat(
    payload: ChatRequest,
    db: Session = Depends(get_db),
    readonly_db: Session = Depends(get_readonly_db),
    _: None = Depends(require_api_key),
):
    try:
        client = db.get(Client, payload.client_id)
        if client is None:
            raise HTTPException(status_code=404, detail="Client not found.")
        if client.status != ClientStatus.ACTIVE:
            raise HTTPException(status_code=403, detail="Client account is not active.")

        response = run_report_copilot(db, readonly_db, payload.client_id, payload.query)
        db.commit()
        return response
    except WorkflowUnavailableError as error:
        db.rollback()
        raise HTTPException(
            status_code=503,
            detail="The report retrieval service is temporarily unavailable.",
        ) from error
    except SQLAlchemyError as error:
        db.rollback()
        raise HTTPException(
            status_code=503,
            detail="The database service is temporarily unavailable.",
        ) from error


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
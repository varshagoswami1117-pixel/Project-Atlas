from __future__ import annotations

import logging
import re
import threading
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import chromadb
from chromadb.api.models.Collection import Collection
from pypdf import PdfReader
from pypdf.errors import PdfReadError
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from backend.database import (
    Client,
    ClientPurchase,
    ClientStatus,
    IngestionJob,
    IngestionJobStatus,
    IngestionStatus,
    PurchaseStatus,
    Report,
)

logger = logging.getLogger(__name__)

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
ALLOWED_SOURCE_ROOTS = (
    (WORKSPACE_ROOT / "data").resolve(),
    (WORKSPACE_ROOT / "docs").resolve(),
)
VECTOR_STORE_PATH = (WORKSPACE_ROOT / "data" / "vector_store").resolve()
COLLECTION_NAME = "atlas_report_chunks"
CHUNK_SIZE = 1200
CHUNK_OVERLAP = 150
MAX_DOCUMENT_BYTES = 50 * 1024 * 1024
MAX_RETRIEVAL_RESULTS = 20

_vector_lock = threading.RLock()
_vector_client: chromadb.PersistentClient | None = None
_collection: Collection | None = None


class IngestionError(ValueError):
    """Raised when a document cannot safely be ingested."""


@dataclass(frozen=True)
class DocumentPage:
    page: int
    text: str


class _SafeHTMLTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._hidden_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() in {"script", "style", "noscript"}:
            self._hidden_depth += 1
        elif tag.lower() in {"br", "p", "div", "li", "tr", "h1", "h2", "h3"}:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style", "noscript"} and self._hidden_depth:
            self._hidden_depth -= 1
        elif tag.lower() in {"p", "div", "li", "tr", "h1", "h2", "h3"}:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self._hidden_depth:
            self.parts.append(data)


def _get_collection() -> Collection:
    global _vector_client, _collection
    with _vector_lock:
        if _collection is None:
            VECTOR_STORE_PATH.mkdir(parents=True, exist_ok=True)
            _vector_client = chromadb.PersistentClient(path=str(VECTOR_STORE_PATH))
            _collection = _vector_client.get_or_create_collection(
                name=COLLECTION_NAME,
                metadata={"hnsw:space": "cosine"},
            )
        return _collection


def _resolve_source_path(source_path: str | None, report: Report) -> Path:
    configured_path = source_path if source_path is not None else report.storage_path
    if "\x00" in configured_path:
        raise IngestionError("The document path is invalid.")
    candidate = Path(configured_path)
    if not candidate.is_absolute():
        candidate = WORKSPACE_ROOT / candidate
    try:
        resolved = candidate.resolve()
    except (OSError, RuntimeError, ValueError) as error:
        raise IngestionError("The document path is invalid.") from error

    if not any(resolved.is_relative_to(root) for root in ALLOWED_SOURCE_ROOTS):
        raise IngestionError("The document must be located inside the workspace data/ or docs/ folder.")
    try:
        if not resolved.is_file():
            raise IngestionError("The requested document file does not exist.")
        if resolved.stat().st_size > MAX_DOCUMENT_BYTES:
            raise IngestionError("The document exceeds the 50 MiB ingestion limit.")
    except OSError as error:
        raise IngestionError("The document file cannot be accessed.") from error
    if resolved.suffix.lower() not in {".pdf", ".md", ".markdown", ".html", ".htm"}:
        raise IngestionError("Only PDF, Markdown, and HTML documents are supported.")
    return resolved


def _normalize_text(value: str) -> str:
    value = value.replace("\x00", " ")
    value = re.sub(r"[ \t]+", " ", value)
    value = re.sub(r"\n[ \t]+\n", "\n\n", value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    return value.strip()


def _read_pdf(path: Path) -> list[DocumentPage]:
    reader = PdfReader(str(path), strict=True)
    pages = [
        DocumentPage(page=index, text=_normalize_text(pdf_page.extract_text() or ""))
        for index, pdf_page in enumerate(reader.pages, start=1)
    ]
    return [page for page in pages if page.text]


def _read_markdown(path: Path) -> list[DocumentPage]:
    content = path.read_text(encoding="utf-8-sig")
    page_marker = re.compile(r"(?m)^\s*<!--\s*page\s*:\s*(\d+)\s*-->\s*$", re.IGNORECASE)
    pages: list[DocumentPage] = []
    current_page = 1
    start = 0

    for match in page_marker.finditer(content):
        page_text = _normalize_text(content[start : match.start()])
        if page_text:
            pages.append(DocumentPage(page=current_page, text=page_text))
        current_page = int(match.group(1))
        if current_page < 1:
            raise IngestionError("Markdown page markers must be positive integers.")
        start = match.end()

    final_text = _normalize_text(content[start:])
    if final_text:
        pages.append(DocumentPage(page=current_page, text=final_text))
    return pages


def _read_html(path: Path) -> list[DocumentPage]:
    parser = _SafeHTMLTextParser()
    parser.feed(path.read_text(encoding="utf-8-sig"))
    parser.close()
    page_text = _normalize_text("".join(parser.parts))
    return [DocumentPage(page=1, text=page_text)] if page_text else []


def _read_document(path: Path) -> list[DocumentPage]:
    try:
        suffix = path.suffix.lower()
        if suffix == ".pdf":
            pages = _read_pdf(path)
        elif suffix in {".md", ".markdown"}:
            pages = _read_markdown(path)
        else:
            pages = _read_html(path)
    except (OSError, UnicodeError, ValueError, PdfReadError) as error:
        raise IngestionError("The document could not be parsed.") from error

    if not pages:
        raise IngestionError("The document contains no extractable text.")
    return pages


def _split_page(text_value: str) -> list[str]:
    if len(text_value) <= CHUNK_SIZE:
        return [text_value] if text_value else []

    chunks: list[str] = []
    start = 0
    text_length = len(text_value)
    while start < text_length:
        end = min(start + CHUNK_SIZE, text_length)
        if end < text_length:
            boundary = max(
                text_value.rfind("\n\n", start, end),
                text_value.rfind(". ", start, end),
                text_value.rfind(" ", start, end),
            )
            if boundary > start + CHUNK_SIZE // 2:
                end = boundary + (2 if text_value[boundary : boundary + 2] == "\n\n" else 1)
        chunk = text_value[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end >= text_length:
            break
        start = max(end - CHUNK_OVERLAP, start + 1)
    return chunks


def _build_chunks(
    pages: list[DocumentPage],
    *,
    report_id: int,
    sector: str,
    region: str,
    year: int,
) -> tuple[list[str], list[dict[str, int | str]], list[str]]:
    documents: list[str] = []
    metadatas: list[dict[str, int | str]] = []
    ids: list[str] = []
    chunk_index = 0

    for page in pages:
        for chunk in _split_page(page.text):
            documents.append(chunk)
            metadatas.append(
                {
                    "report_id": report_id,
                    "sector": sector,
                    "region": region,
                    "year": year,
                    "page": page.page,
                }
            )
            ids.append(f"report-{report_id}-chunk-{chunk_index}")
            chunk_index += 1
    return ids, documents, metadatas


def get_authorized_report_ids(db: Session, client_id: int) -> list[int]:
    statement = (
        select(ClientPurchase.report_id)
        .join(Client, Client.id == ClientPurchase.client_id)
        .where(
            Client.id == client_id,
            Client.status == ClientStatus.ACTIVE,
            ClientPurchase.status == PurchaseStatus.ACTIVE,
            or_(
                ClientPurchase.expires_at.is_(None),
                ClientPurchase.expires_at > func.current_timestamp(),
            ),
        )
        .order_by(ClientPurchase.report_id)
    )
    return list(db.scalars(statement).all())


def retrieve_authorized_chunks(
    query: str,
    purchased_reports: list[int],
    *,
    limit: int = 5,
) -> list[dict[str, Any]]:
    cleaned_query = query.strip()
    if not cleaned_query:
        raise ValueError("The retrieval query cannot be empty.")
    if not 1 <= limit <= MAX_RETRIEVAL_RESULTS:
        raise ValueError(f"limit must be between 1 and {MAX_RETRIEVAL_RESULTS}.")

    if any(type(report_id) is not int or report_id <= 0 for report_id in purchased_reports):
        raise ValueError("purchased_reports must contain only positive integer report IDs.")
    authorized_ids = sorted(set(purchased_reports))
    if not authorized_ids:
        return []

    result = _get_collection().query(
        query_texts=[cleaned_query],
        n_results=limit,
        where={"report_id": {"$in": authorized_ids}},
        include=["documents", "metadatas", "distances"],
    )
    documents = result["documents"][0]
    metadatas = result["metadatas"][0]
    distances = result["distances"][0]
    return [
        {"text": document, "metadata": metadata, "distance": distance}
        for document, metadata, distance in zip(documents, metadatas, distances)
        if document is not None and metadata is not None and metadata["report_id"] in authorized_ids
    ]


def ingest_report(
    db: Session,
    *,
    report_id: int,
    sector: str,
    region: str,
    year: int,
    source_path: str | None = None,
) -> dict[str, int | str]:
    report = db.get(Report, report_id)
    if report is None:
        raise LookupError("The requested report does not exist.")

    job = IngestionJob(
        report_id=report_id,
        status=IngestionJobStatus.RUNNING,
        attempt_count=1,
        started_at=func.current_timestamp(),
    )
    report.ingestion_status = IngestionStatus.PROCESSING
    db.add(job)
    db.flush()
    job_id = job.id
    db.commit()

    try:
        resolved_path = _resolve_source_path(source_path, report)
        pages = _read_document(resolved_path)
        ids, documents, metadatas = _build_chunks(
            pages,
            report_id=report_id,
            sector=sector,
            region=region,
            year=year,
        )
        if not documents:
            raise IngestionError("The document contains no chunks to index.")

        collection = _get_collection()
        with _vector_lock:
            batch_size = 256
            for start in range(0, len(ids), batch_size):
                end = start + batch_size
                collection.upsert(
                    ids=ids[start:end],
                    documents=documents[start:end],
                    metadatas=metadatas[start:end],
                )
            existing = collection.get(where={"report_id": report_id})
            indexed_ids = set(ids)
            stale_ids = [chunk_id for chunk_id in existing["ids"] if chunk_id not in indexed_ids]
            if stale_ids:
                collection.delete(ids=stale_ids)
        report.ingestion_status = IngestionStatus.COMPLETED
        job.status = IngestionJobStatus.COMPLETED
        job.finished_at = func.current_timestamp()
        db.commit()
    except IngestionError as error:
        db.rollback()
        failed_job = db.get(IngestionJob, job_id)
        failed_report = db.get(Report, report_id)
        if failed_job is not None:
            failed_job.status = IngestionJobStatus.FAILED
            failed_job.error_message = str(error)
            failed_job.finished_at = func.current_timestamp()
        if failed_report is not None:
            failed_report.ingestion_status = IngestionStatus.FAILED
        db.commit()
        raise
    except Exception as error:
        db.rollback()
        failed_job = db.get(IngestionJob, job_id)
        failed_report = db.get(Report, report_id)
        if failed_job is not None:
            failed_job.status = IngestionJobStatus.FAILED
            failed_job.error_message = "Vector indexing failed."
            failed_job.finished_at = func.current_timestamp()
        if failed_report is not None:
            failed_report.ingestion_status = IngestionStatus.FAILED
        db.commit()
        logger.exception("Document ingestion failed for report_id=%s", report_id)
        raise RuntimeError("The report could not be added to the vector store.") from error

    logger.info(
        "Ingested report_id=%s chunks=%s source=%s",
        report_id,
        len(documents),
        resolved_path.name,
    )
    return {"report_id": report_id, "chunks_indexed": len(documents), "job_id": job_id}

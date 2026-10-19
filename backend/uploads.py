from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from sqlalchemy.orm import Session

from backend.database import Report
from backend.ingestion import WORKSPACE_ROOT, IngestionError, ingest_report

MAX_UPLOAD_BYTES = 25 * 1024 * 1024
ALLOWED_UPLOAD_SUFFIXES = {".pdf", ".md", ".markdown", ".html", ".htm"}
UPLOAD_DIRECTORY = (WORKSPACE_ROOT / "data" / "uploads").resolve()


def _safe_filename(filename: str) -> tuple[str, str]:
    basename = filename.replace("\\", "/").rsplit("/", 1)[-1]
    basename = "".join(character for character in basename if character.isprintable())
    basename = basename.strip()[:255]
    suffix = Path(basename).suffix.lower()
    if not basename or suffix not in ALLOWED_UPLOAD_SUFFIXES:
        raise IngestionError("Choose a PDF, Markdown, or HTML report file.")
    return basename, suffix


def _validate_upload_content(suffix: str, content: bytes) -> None:
    if not content:
        raise IngestionError("The uploaded report is empty.")
    if len(content) > MAX_UPLOAD_BYTES:
        raise IngestionError("The uploaded report exceeds the 25 MiB limit.")
    if suffix == ".pdf" and not content.startswith(b"%PDF-"):
        raise IngestionError("The uploaded file does not have a valid PDF signature.")
    if suffix in {".md", ".markdown", ".html", ".htm"}:
        try:
            content.decode("utf-8-sig")
        except UnicodeDecodeError as error:
            raise IngestionError("Markdown and HTML uploads must be UTF-8 text.") from error


def ingest_uploaded_report(
    db: Session,
    *,
    report_id: int,
    sector: str,
    region: str,
    year: int,
    filename: str,
    content: bytes,
) -> dict[str, int | str]:
    safe_filename, suffix = _safe_filename(filename)
    _validate_upload_content(suffix, content)

    report = db.get(Report, report_id)
    if report is None:
        raise LookupError("The selected report is not registered in the database.")

    UPLOAD_DIRECTORY.mkdir(parents=True, exist_ok=True)
    destination = (UPLOAD_DIRECTORY / f"report-{report_id}-{uuid4().hex}{suffix}").resolve()
    if not destination.is_relative_to(UPLOAD_DIRECTORY):
        raise IngestionError("The upload destination is invalid.")
    relative_path = destination.relative_to(WORKSPACE_ROOT).as_posix()

    with destination.open("xb") as uploaded_file:
        uploaded_file.write(content)

    report.source_filename = safe_filename
    report.storage_path = relative_path
    try:
        db.commit()
    except Exception:
        db.rollback()
        destination.unlink(missing_ok=True)
        raise

    result = ingest_report(
        db,
        report_id=report_id,
        sector=sector,
        region=region,
        year=year,
        source_path=relative_path,
    )
    return {**result, "source_filename": safe_filename}

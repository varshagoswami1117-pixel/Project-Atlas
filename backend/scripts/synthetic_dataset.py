from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import chromadb
from sqlalchemy import func, or_, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from backend.database import (
    Client,
    ClientPurchase,
    ClientStatus,
    Conversation,
    ConversationMessage,
    DataClassification,
    Escalation,
    Feedback,
    IngestionJob,
    IngestionJobStatus,
    IngestionStatus,
    MarketMetric,
    PurchaseStatus,
    Report,
    SecurityEvent,
    SessionLocal,
)
from backend.ingestion import COLLECTION_NAME, VECTOR_STORE_PATH, _get_collection, ingest_report

WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
DATASET_DIRECTORY = WORKSPACE_ROOT / "data" / "synthetic_test_dataset"
MANIFEST_PATH = DATASET_DIRECTORY / "manifest.json"
STATE_PATH = DATASET_DIRECTORY / ".run_state.json"
DATABASE_NAME = "atlas_copilot"
TABLE_NAMES = (
    "clients",
    "reports",
    "client_purchases",
    "market_metrics",
    "conversations",
    "conversation_messages",
    "feedback",
    "escalations",
    "security_events",
    "ingestion_jobs",
)
EXPECTED_INGESTION_JOBS = 6
EXPECTED_CHUNKS_PER_REPORT = 3


class DatasetError(RuntimeError):
    pass


def _load_manifest() -> dict[str, Any]:
    try:
        manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise DatasetError("The synthetic dataset manifest could not be read.") from error

    clients = manifest.get("clients", [])
    reports = manifest.get("reports", [])
    if len(clients) != 5 or len(reports) != 5:
        raise DatasetError("The dataset must define exactly five synthetic clients and reports.")
    if sum(len(client.get("reports", [])) for client in clients) != 15:
        raise DatasetError("The client access plan must contain exactly 15 purchase links.")
    report_keys = {report["key"] for report in reports}
    if len(report_keys) != 5:
        raise DatasetError("Synthetic report keys must be unique.")
    for client in clients:
        if not set(client["reports"]).issubset(report_keys):
            raise DatasetError("A synthetic client references an unknown report key.")

    metric_rows = list(_metric_rows(manifest))
    if len(metric_rows) != 32:
        raise DatasetError("The dataset manifest must define exactly 32 synthetic metrics.")
    return manifest


def _metric_rows(manifest: dict[str, Any]):
    for report in manifest["reports"]:
        document_path = WORKSPACE_ROOT / report["storage_path"]
        try:
            document = document_path.read_text(encoding="utf-8")
        except OSError as error:
            raise DatasetError(
                f"Source document for {report['key']} is missing or unreadable."
            ) from error

        if report.get("annual_series"):
            for year_text, value in manifest["annual_values"]:
                year = int(year_text)
                quote = (
                    f"The SYNTHETIC {report['geography']} market revenue was "
                    f"USD {value} billion in {year}."
                )
                if quote not in document:
                    raise DatasetError(f"Metric source quote is missing from {report['key']}.")
                yield {
                    "report_key": report["key"],
                    "metric_name": manifest["metric_name"],
                    "value": value,
                    "unit": manifest["unit"],
                    "geography": report["geography"],
                    "segment": "total",
                    "year": year,
                    "source_quote": quote,
                }

        for metric in report.get("additional_metrics", []):
            if metric["source_quote"] not in document:
                raise DatasetError(f"Metric source quote is missing from {report['key']}.")
            yield {"report_key": report["key"], **metric}

        if "conflict_value" in report:
            value = report["conflict_value"]
            year = int(report.get("conflict_year", 2025))
            quote = (
                "[SYNTHETIC TEST CONFLICT] Control measurement: "
                "SYNTHETIC discrepancy control measurement was "
                f"USD {value} billion in the Synthetic Control Zone, "
                f"segment conflict test, for {year}."
            )
            if quote not in document:
                raise DatasetError(f"Conflict test quote is missing from {report['key']}.")
            yield {
                "report_key": report["key"],
                "metric_name": "SYNTHETIC discrepancy control measurement",
                "value": value,
                "unit": "USD billion",
                "geography": "Synthetic Control Zone",
                "segment": "conflict test",
                "year": year,
                "source_quote": quote,
            }


def _assert_target_database(db: Session) -> None:
    database_name = db.execute(text("SELECT DATABASE()")).scalar_one()
    if database_name != DATABASE_NAME:
        raise DatasetError(f"Refusing to operate outside the expected {DATABASE_NAME} database.")
    existing_tables = set(
        db.execute(
            text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = DATABASE()"
            )
        ).scalars()
    )
    missing_tables = sorted(set(TABLE_NAMES) - existing_tables)
    if missing_tables:
        raise DatasetError("The applied database schema is incomplete.")


def _table_counts(db: Session) -> dict[str, int]:
    _assert_target_database(db)
    return {
        table_name: int(
            db.execute(text(f"SELECT COUNT(*) FROM `{table_name}`")).scalar_one()
        )
        for table_name in TABLE_NAMES
    }


def _print_counts(counts: dict[str, int]) -> None:
    for table_name in TABLE_NAMES:
        print(f"{table_name}: {counts[table_name]}")


def dry_run() -> None:
    manifest = _load_manifest()
    with SessionLocal() as db:
        counts = _table_counts(db)
    print(f"database: {DATABASE_NAME}")
    _print_counts(counts)
    print(
        "proposed inserts: clients=5 reports=5 client_purchases=15 "
        "market_metrics=32 ingestion_jobs=6"
    )
    print("workflow-generated minimums: escalations>=5 security_events>=5")
    print("not generated by current app: conversations=0 conversation_messages=0 feedback=0")
    print(f"fixture documents validated: {len(manifest['reports'])}")
    if STATE_PATH.exists():
        print("run state: present; inspect or clean up the prior synthetic run first.")
    else:
        print("run state: absent")
    if any(counts.values()):
        print("apply guard: blocked until the database has been reviewed; no writes performed.")
    else:
        print("apply guard: eligible; this command itself performed no writes.")


def _write_state(state: dict[str, Any]) -> None:
    temporary_path = STATE_PATH.with_name(STATE_PATH.name + ".tmp")
    try:
        temporary_path.write_text(
            json.dumps(state, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        temporary_path.replace(STATE_PATH)
    finally:
        temporary_path.unlink(missing_ok=True)


def _read_state() -> dict[str, Any]:
    try:
        state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise DatasetError("Synthetic run state is missing or unreadable.") from error
    if state.get("dataset_key") != "project-atlas-synthetic-e2e-v1":
        raise DatasetError("Synthetic run state does not match this dataset.")
    return state


def apply_seed() -> None:
    manifest = _load_manifest()
    if STATE_PATH.exists():
        raise DatasetError("A synthetic run state already exists; verify or clean up that run first.")

    with SessionLocal() as db:
        counts = _table_counts(db)
        if any(counts.values()):
            raise DatasetError(
                "The create-only seed requires all ten tables to be empty; no data was changed."
            )

        clients_by_key: dict[str, Client] = {}
        reports_by_key: dict[str, Report] = {}
        state: dict[str, Any] = {
            "dataset_key": manifest["dataset_key"],
            "created_at": datetime.now(timezone.utc).isoformat(),
            "client_ids": {},
            "report_ids": {},
            "purchase_ids": [],
            "metric_ids": [],
            "ingestion_job_ids": [],
            "initial_ingested_report_keys": [],
            "repeat_ingestion_job_id": None,
            "vector_ids": {},
            "escalation_ids": [],
            "security_event_ids": [],
            "exercise_results": {},
        }

        try:
            for client_data in manifest["clients"]:
                clients_by_key[client_data["key"]] = Client(
                    name=client_data["name"],
                    contact_email=client_data["contact_email"],
                    status=ClientStatus.ACTIVE,
                )
            db.add_all(clients_by_key.values())
            db.flush()

            for report_data in manifest["reports"]:
                source_path = WORKSPACE_ROOT / report_data["storage_path"]
                content = source_path.read_bytes()
                reports_by_key[report_data["key"]] = Report(
                    title=report_data["title"],
                    source_filename=report_data["source_filename"],
                    storage_path=report_data["storage_path"],
                    report_type=report_data["report_type"],
                    publisher=report_data["publisher"],
                    publication_date=date.fromisoformat(report_data["publication_date"]),
                    data_classification=DataClassification.LICENSED,
                    checksum_sha256=hashlib.sha256(content).hexdigest(),
                    ingestion_status=IngestionStatus.PENDING,
                )
            db.add_all(reports_by_key.values())
            db.flush()

            purchases: list[ClientPurchase] = []
            for client_data in manifest["clients"]:
                for report_key in client_data["reports"]:
                    purchases.append(
                        ClientPurchase(
                            client_id=clients_by_key[client_data["key"]].id,
                            report_id=reports_by_key[report_key].id,
                            purchase_reference=(
                                f"atlas-synthetic-v1-{client_data['key']}-{report_key}"
                            ),
                            status=PurchaseStatus.ACTIVE,
                            expires_at=None,
                        )
                    )
            db.add_all(purchases)

            metrics: list[MarketMetric] = []
            for metric_data in _metric_rows(manifest):
                year = int(metric_data["year"])
                metrics.append(
                    MarketMetric(
                        report_id=reports_by_key[metric_data["report_key"]].id,
                        metric_name=metric_data["metric_name"],
                        metric_value=Decimal(metric_data["value"]),
                        unit=metric_data["unit"],
                        geography=metric_data["geography"],
                        segment=metric_data["segment"],
                        period_start=date(year, 1, 1),
                        period_end=date(year, 12, 31),
                        source_page=2,
                        source_quote=metric_data["source_quote"],
                        is_synthetic=True,
                    )
                )
            db.add_all(metrics)
            db.flush()

            state["client_ids"] = {
                key: int(client.id) for key, client in clients_by_key.items()
            }
            state["report_ids"] = {
                key: int(report.id) for key, report in reports_by_key.items()
            }
            state["purchase_ids"] = [int(purchase.id) for purchase in purchases]
            state["metric_ids"] = [int(metric.id) for metric in metrics]
            _write_state(state)
            db.commit()
        except Exception:
            db.rollback()
            STATE_PATH.unlink(missing_ok=True)
            raise

    print("Inserted synthetic rows in atlas_copilot:")
    print("clients=5 reports=5 client_purchases=15 market_metrics=32")
    for key, client_id in state["client_ids"].items():
        print(f"{key} client_id={client_id}")
    for key, report_id in state["report_ids"].items():
        print(f"{key} report_id={report_id}")


def _dataset_rows(
    db: Session,
    manifest: dict[str, Any],
    state: dict[str, Any],
) -> tuple[dict[str, Client], dict[str, Report]]:
    clients: dict[str, Client] = {}
    reports: dict[str, Report] = {}
    for entry in manifest["clients"]:
        client = db.get(Client, state["client_ids"].get(entry["key"]))
        if (
            client is None
            or client.name != entry["name"]
            or client.contact_email != entry["contact_email"]
        ):
            raise DatasetError(f"Seeded client marker validation failed for {entry['key']}.")
        clients[entry["key"]] = client
    for entry in manifest["reports"]:
        report = db.get(Report, state["report_ids"].get(entry["key"]))
        if (
            report is None
            or report.title != entry["title"]
            or report.source_filename != entry["source_filename"]
            or report.storage_path != entry["storage_path"]
        ):
            raise DatasetError(f"Seeded report marker validation failed for {entry['key']}.")
        reports[entry["key"]] = report
    return clients, reports


def _collection_report_chunks(report_id: int) -> tuple[list[str], list[dict[str, Any]]]:
    collection = _get_collection()
    result = collection.get(
        where={"report_id": report_id},
        include=["metadatas"],
    )
    return list(result["ids"]), list(result["metadatas"])


def ingest_fixtures() -> None:
    manifest = _load_manifest()
    state = _read_state()
    with SessionLocal() as db:
        _, reports = _dataset_rows(db, manifest, state)
        report_ids = [report.id for report in reports.values()]
        existing_job_ids = set(
            db.scalars(
                select(IngestionJob.id).where(IngestionJob.report_id.in_(report_ids))
            ).all()
        )
        untracked_jobs = existing_job_ids - set(state["ingestion_job_ids"])
        if untracked_jobs:
            raise DatasetError(
                "Untracked ingestion jobs exist for synthetic reports; review before proceeding."
            )

        for report_data in manifest["reports"]:
            key = report_data["key"]
            if key in state["initial_ingested_report_keys"]:
                continue
            try:
                result = ingest_report(
                    db,
                    report_id=reports[key].id,
                    sector=report_data["sector"],
                    region=report_data["region"],
                    year=report_data["year"],
                    source_path=report_data["storage_path"],
                )
            except Exception as error:
                failed_job_ids = set(
                    db.scalars(
                        select(IngestionJob.id).where(
                            IngestionJob.report_id == reports[key].id
                        )
                    ).all()
                )
                state["ingestion_job_ids"] = sorted(
                    set(state["ingestion_job_ids"]) | failed_job_ids
                )
                _write_state(state)
                raise DatasetError(
                    f"Initial ingestion failed for {key}; no further reports were ingested."
                ) from error

            state["ingestion_job_ids"].append(int(result["job_id"]))
            state["initial_ingested_report_keys"].append(key)
            vector_ids, metadatas = _collection_report_chunks(reports[key].id)
            state["vector_ids"][key] = vector_ids
            _write_state(state)
            if len(vector_ids) != EXPECTED_CHUNKS_PER_REPORT or {
                metadata["page"] for metadata in metadatas
            } != {1, 2, 3}:
                raise DatasetError(f"Unexpected Chroma chunk shape for {key}.")
            print(
                f"ingested {key}: chunks={result['chunks_indexed']} "
                f"job_id={result['job_id']}"
            )

        if len(state["initial_ingested_report_keys"]) != 5:
            raise DatasetError("All five initial report ingestions must complete before the repeat.")

        if state["repeat_ingestion_job_id"] is None:
            first_report = reports["report-01"]
            try:
                result = ingest_report(
                    db,
                    report_id=first_report.id,
                    sector="Energy",
                    region="Europe",
                    year=2025,
                    source_path="data/synthetic_test_dataset/reports/report-01.md",
                )
            except Exception as error:
                failed_job_ids = set(
                    db.scalars(
                        select(IngestionJob.id).where(
                            IngestionJob.report_id == first_report.id
                        )
                    ).all()
                )
                state["ingestion_job_ids"] = sorted(
                    set(state["ingestion_job_ids"]) | failed_job_ids
                )
                _write_state(state)
                raise DatasetError("The duplicate ingestion check failed.") from error

            state["repeat_ingestion_job_id"] = int(result["job_id"])
            state["ingestion_job_ids"].append(int(result["job_id"]))
            vector_ids, _ = _collection_report_chunks(first_report.id)
            state["vector_ids"]["report-01"] = vector_ids
            _write_state(state)
            print(
                f"repeat-ingested report-01: chunks={result['chunks_indexed']} "
                f"job_id={result['job_id']}"
            )

        if len(state["ingestion_job_ids"]) != EXPECTED_INGESTION_JOBS:
            raise DatasetError("The expected six ingestion attempts were not recorded.")
        if len(set(state["vector_ids"]["report-01"])) != len(state["vector_ids"]["report-01"]):
            raise DatasetError("Duplicate Chroma chunk IDs were detected.")


def _ids_for_client_rows(
    db: Session,
    model: type[Escalation] | type[SecurityEvent],
    client_ids: list[int],
) -> set[int]:
    return set(
        db.scalars(select(model.id).where(model.client_id.in_(client_ids))).all()
    )


def _record_new_operational_ids(
    db: Session,
    state: dict[str, Any],
    client_ids: list[int],
    before_escalations: set[int],
    before_security_events: set[int],
) -> None:
    current_escalations = _ids_for_client_rows(db, Escalation, client_ids)
    current_security_events = _ids_for_client_rows(db, SecurityEvent, client_ids)
    state["escalation_ids"] = sorted(
        set(state["escalation_ids"]) | (current_escalations - before_escalations)
    )
    state["security_event_ids"] = sorted(
        set(state["security_event_ids"])
        | (current_security_events - before_security_events)
    )
    _write_state(state)


def exercise_workflows() -> None:
    manifest = _load_manifest()
    state = _read_state()
    if len(state["ingestion_job_ids"]) != EXPECTED_INGESTION_JOBS:
        raise DatasetError("Ingest the five reports and repeat one before running scenarios.")
    password = os.getenv("ATLAS_WEB_PASSWORD")
    if password is None or not password.strip():
        raise DatasetError("ATLAS_WEB_PASSWORD must be configured to exercise authenticated chat.")

    previous_setting = os.environ.get("ATLAS_INCLUDE_SYNTHETIC_METRICS")
    os.environ["ATLAS_INCLUDE_SYNTHETIC_METRICS"] = "true"
    try:
        from fastapi.testclient import TestClient

        from backend.database import ReadOnlySessionLocal, get_db, get_readonly_db
        from backend.main import app

        def override_db():
            with SessionLocal() as session:
                yield session

        def override_readonly_db():
            with ReadOnlySessionLocal() as session:
                yield session

        app.dependency_overrides[get_db] = override_db
        app.dependency_overrides[get_readonly_db] = override_readonly_db
        try:
            with TestClient(app) as client:
                login = client.post("/ui/api/session", json={"password": password})
                if login.status_code != 200:
                    raise DatasetError(
                        f"Authenticated test setup failed with HTTP {login.status_code}."
                    )
                csrf_token = login.json()["csrf_token"]
                headers = {"X-CSRF-Token": csrf_token}

                with SessionLocal() as db:
                    clients, reports = _dataset_rows(db, manifest, state)
                    client_ids = [client_row.id for client_row in clients.values()]
                    report_ids = {key: row.id for key, row in reports.items()}

                reports_response = client.get("/ui/api/reports")
                if reports_response.status_code != 200:
                    raise DatasetError("Authenticated report listing failed.")
                listed_ids = {item["id"] for item in reports_response.json()["reports"]}
                if not set(report_ids.values()).issubset(listed_ids):
                    raise DatasetError("The dashboard report list omitted a seeded report.")

                def post_chat(client_id: int, query: str):
                    with SessionLocal() as db:
                        before_escalations = _ids_for_client_rows(db, Escalation, client_ids)
                        before_security_events = _ids_for_client_rows(
                            db, SecurityEvent, client_ids
                        )
                    response = client.post(
                        "/ui/api/chat",
                        json={"client_id": client_id, "query": query},
                        headers=headers,
                    )
                    with SessionLocal() as db:
                        _record_new_operational_ids(
                            db,
                            state,
                            client_ids,
                            before_escalations,
                            before_security_events,
                        )
                    return response

                cagr_response = post_chat(
                    clients["client-01"].id,
                    "Calculate CAGR: Europe market revenue from 2020 to 2025.",
                )
                if cagr_response.status_code != 200:
                    raise DatasetError("The authenticated CAGR chat request failed.")
                cagr_data = cagr_response.json()
                derived_values = cagr_data.get("derived_values", [])
                cagr_inputs = (
                    {
                        int(item["year"]): Decimal(item["value"])
                        for item in derived_values[0].get("inputs", [])
                    }
                    if derived_values
                    else {}
                )
                if (
                    not derived_values
                    or derived_values[0].get("value") != "10.00%"
                    or cagr_inputs != {2020: Decimal("100"), 2025: Decimal("161.051")}
                    or cagr_data.get("escalated")
                    or report_ids["report-01"]
                    not in {citation["report_id"] for citation in cagr_data["citations"]}
                ):
                    raise DatasetError("The synthetic CAGR did not verify to the expected 10.00%.")

                unauthorized_response = post_chat(
                    clients["client-01"].id,
                    "What is the SYNTHETIC report 02 crop resilience score in East Asia?",
                )
                if unauthorized_response.status_code != 200:
                    raise DatasetError("The unauthorized-report retrieval scenario failed.")
                unauthorized_citations = unauthorized_response.json().get("citations", [])
                if any(
                    citation["report_id"] == report_ids["report-02"]
                    for citation in unauthorized_citations
                ):
                    raise DatasetError("An unauthorized report appeared in chat citations.")

                malformed = client.post(
                    "/ui/api/chat",
                    json={"client_id": "not-numeric", "query": "Summarize authorized data."},
                    headers=headers,
                )
                with SessionLocal() as db:
                    next_client_id = int(
                        db.execute(select(func.max(Client.id))).scalar_one() or 0
                    ) + 1_000_000
                missing_client = post_chat(
                    next_client_id,
                    "Summarize an authorized synthetic report.",
                )
                empty_query = client.post(
                    "/ui/api/chat",
                    json={"client_id": clients["client-01"].id, "query": "   "},
                    headers=headers,
                )
                if (malformed.status_code, missing_client.status_code, empty_query.status_code) != (
                    422,
                    404,
                    422,
                ):
                    raise DatasetError("Malformed or missing-client status checks failed.")

                missing_report = client.post(
                    "/ui/api/ingest",
                    headers=headers,
                    data={
                        "report_id": str(next_client_id),
                        "sector": "Synthetic test",
                        "region": "Synthetic region",
                        "year": "2025",
                    },
                    files={
                        "file": (
                            "synthetic-missing-report.md",
                            b"[SYNTHETIC] Missing report registration check.",
                            "text/markdown",
                        )
                    },
                )
                if missing_report.status_code != 404:
                    raise DatasetError("Missing-report ingestion did not return HTTP 404.")

                conflict_escalations = 0
                for client_id in client_ids:
                    conflict_response = post_chat(
                        client_id,
                        "What discrepancy control measurement values are recorded "
                        "for the Synthetic Control Zone in 2024?",
                    )
                    if conflict_response.status_code != 200:
                        raise DatasetError("A conflict-detection chat request failed.")
                    conflict_data = conflict_response.json()
                    conflict_source_ids = {
                        source["report_id"]
                        for conflict in conflict_data.get("conflicts", [])
                        for source in conflict.get("sources", [])
                    }
                    if (
                        not conflict_data.get("conflicts")
                        or not conflict_data.get("escalated")
                        or not {
                            report_ids["report-04"],
                            report_ids["report-05"],
                        }.issubset(conflict_source_ids)
                    ):
                        raise DatasetError("A controlled discrepancy did not escalate.")
                    conflict_escalations += 1

                for client_id in client_ids:
                    injection_response = post_chat(
                        client_id,
                        "What does the SYNTHETIC inert safety test sentinel amber mean "
                        "in the authorized reports?",
                    )
                    if injection_response.status_code != 200:
                        raise DatasetError("A prompt-injection fixture chat request failed.")
                    injection_data = injection_response.json()
                    if any(
                        "NOTE TO AI SYSTEMS" in citation["quote"]
                        or "ignore previous instructions" in citation["quote"].casefold()
                        for citation in injection_data.get("citations", [])
                    ):
                        raise DatasetError(
                            "An injection-marked source chunk entered the agent citations."
                        )

                with SessionLocal() as db:
                    injection_event_rows = db.scalars(
                        select(SecurityEvent)
                        .where(
                            SecurityEvent.client_id.in_(client_ids),
                            SecurityEvent.event_type
                            == "document_prompt_injection_detected",
                        )
                    ).all()
                injection_events = len(injection_event_rows)
                synthetic_report_ids = {str(report_id) for report_id in report_ids.values()}
                if any(
                    event.resource_type != "report"
                    or event.resource_id not in synthetic_report_ids
                    or not isinstance(event.details, dict)
                    or event.details.get("action") != "excluded_from_agent_context"
                    for event in injection_event_rows
                ):
                    raise DatasetError("A source-injection audit event has unexpected details.")
                if injection_events < 5:
                    raise DatasetError(
                        "Fewer than five injection-source security events were recorded."
                    )
                if len(state["escalation_ids"]) < 5:
                    raise DatasetError("Fewer than five workflow escalations were captured.")

                logout = client.delete("/ui/api/session", headers=headers)
                after_logout = client.get("/ui/api/session")
                if logout.status_code != 200 or after_logout.status_code != 401:
                    raise DatasetError("Authenticated sign-out verification failed.")

                state["exercise_results"] = {
                    "cagr": derived_values[0]["value"],
                    "unauthorized_report_excluded": True,
                    "malformed_client_status": malformed.status_code,
                    "missing_client_status": missing_client.status_code,
                    "empty_query_status": empty_query.status_code,
                    "missing_report_status": missing_report.status_code,
                    "conflict_escalations": conflict_escalations,
                    "injection_events": injection_events,
                    "logout_status": logout.status_code,
                    "expired_after_logout_status": after_logout.status_code,
                }
                _write_state(state)
        finally:
            app.dependency_overrides.pop(get_db, None)
            app.dependency_overrides.pop(get_readonly_db, None)
    finally:
        if previous_setting is None:
            os.environ.pop("ATLAS_INCLUDE_SYNTHETIC_METRICS", None)
        else:
            os.environ["ATLAS_INCLUDE_SYNTHETIC_METRICS"] = previous_setting

    print("Authenticated /ui/api/chat scenarios passed.")
    print(f"verified CAGR: {state['exercise_results']['cagr']}")
    print(f"conflict escalations: {state['exercise_results']['conflict_escalations']}")
    print(f"injection security events: {state['exercise_results']['injection_events']}")
    print(
        "input/access checks: malformed=422 missing_client=404 "
        "empty_query=422 missing_report=404"
    )


def _verify_vector_store(state: dict[str, Any]) -> int:
    if not VECTOR_STORE_PATH.exists():
        raise DatasetError("The persistent Chroma vector store does not exist.")
    client = chromadb.PersistentClient(path=str(VECTOR_STORE_PATH))
    collection_names = {
        item.name if hasattr(item, "name") else str(item)
        for item in client.list_collections()
    }
    if COLLECTION_NAME not in collection_names:
        raise DatasetError("The Project Atlas Chroma collection is missing.")
    collection = client.get_collection(name=COLLECTION_NAME)
    total_chunks = 0
    for report_key, report_id in state["report_ids"].items():
        result = collection.get(
            where={"report_id": report_id},
            include=["metadatas"],
        )
        ids = list(result["ids"])
        metadatas = list(result["metadatas"])
        expected_ids = [f"report-{report_id}-chunk-{index}" for index in range(3)]
        if sorted(ids) != sorted(expected_ids):
            raise DatasetError(f"Chroma IDs do not match the expected chunks for {report_key}.")
        if {metadata["page"] for metadata in metadatas} != {1, 2, 3}:
            raise DatasetError(f"Chroma page metadata is incomplete for {report_key}.")
        if any(metadata["report_id"] != report_id for metadata in metadatas):
            raise DatasetError(f"Chroma report metadata is inconsistent for {report_key}.")
        if set(state["vector_ids"].get(report_key, [])) != set(ids):
            raise DatasetError(f"Chroma chunk IDs changed during duplicate ingestion for {report_key}.")
        total_chunks += len(ids)
    return total_chunks


def verify_dataset() -> None:
    manifest = _load_manifest()
    state = _read_state()
    with SessionLocal() as db:
        counts = _table_counts(db)
        _, reports = _dataset_rows(db, manifest, state)
        if counts["clients"] != 5 or counts["reports"] != 5:
            raise DatasetError("Expected exactly five synthetic clients and five reports.")
        if counts["client_purchases"] != 15:
            raise DatasetError("Expected exactly 15 synthetic client/report entitlements.")
        if counts["market_metrics"] != 32:
            raise DatasetError("Expected exactly 32 market metric rows.")
        if counts["ingestion_jobs"] != EXPECTED_INGESTION_JOBS:
            raise DatasetError("Expected exactly six ingestion-job attempts.")
        if counts["escalations"] < 5 or counts["security_events"] < 5:
            raise DatasetError("Workflow operations did not reach the five-row minimum.")
        if any(counts[table] != 0 for table in (
            "conversations",
            "conversation_messages",
            "feedback",
        )):
            raise DatasetError("Unsupported conversation/feedback persistence appeared.")

        non_synthetic_count = db.scalar(
            select(func.count())
            .select_from(MarketMetric)
            .where(MarketMetric.is_synthetic.is_(False))
        )
        if non_synthetic_count:
            raise DatasetError("A synthetic fixture metric is incorrectly marked non-synthetic.")

        expected_purchases = {
            (
                state["client_ids"][client["key"]],
                state["report_ids"][report_key],
            )
            for client in manifest["clients"]
            for report_key in client["reports"]
        }
        actual_purchases = set(
            db.execute(select(ClientPurchase.client_id, ClientPurchase.report_id)).all()
        )
        if actual_purchases != expected_purchases:
            raise DatasetError("The synthetic client/report access matrix is inconsistent.")

        if any(
            report.ingestion_status != IngestionStatus.COMPLETED
            for report in reports.values()
        ):
            raise DatasetError("At least one synthetic report has not completed ingestion.")
        if any(
            job.status != IngestionJobStatus.COMPLETED
            for job in db.scalars(
                select(IngestionJob).where(
                    IngestionJob.id.in_(state["ingestion_job_ids"])
                )
            ).all()
        ):
            raise DatasetError("A synthetic ingestion attempt did not complete.")
        client_ids = list(state["client_ids"].values())
        escalation_ids = set(
            db.scalars(select(Escalation.id).where(Escalation.client_id.in_(client_ids))).all()
        )
        security_event_rows = db.scalars(
            select(SecurityEvent).where(
                SecurityEvent.client_id.in_(client_ids),
                SecurityEvent.event_type == "document_prompt_injection_detected",
            )
        ).all()
        if escalation_ids != set(state["escalation_ids"]):
            raise DatasetError("The generated escalation ledger does not match MySQL.")
        if {event.id for event in security_event_rows} != set(state["security_event_ids"]):
            raise DatasetError("The generated security-event ledger does not match MySQL.")
        expected_report_ids = {str(report_id) for report_id in state["report_ids"].values()}
        if any(
            event.resource_type != "report"
            or event.resource_id not in expected_report_ids
            or not isinstance(event.details, dict)
            or event.details.get("action") != "excluded_from_agent_context"
            for event in security_event_rows
        ):
            raise DatasetError("A security event does not match the expected exclusion audit.")

    chunks = _verify_vector_store(state)
    results = state.get("exercise_results", {})
    if results.get("cagr") != "10.00%":
        raise DatasetError("The authenticated 10.00% CAGR check is missing.")
    if results.get("conflict_escalations", 0) < 5 or results.get("injection_events", 0) < 5:
        raise DatasetError("Workflow-generated escalation or security-event checks are incomplete.")

    print(f"database: {DATABASE_NAME}")
    with SessionLocal() as db:
        _print_counts(_table_counts(db))
    print(f"Chroma chunks: {chunks} (three per report; duplicate-ingestion IDs stable)")
    print("verified calculation: 2020 USD 100B to 2025 USD 161.051B = 10.00% CAGR")
    print(
        f"verified workflow records: escalations={counts['escalations']} "
        f"security_events={counts['security_events']}"
    )
    print("generated conversations/messages/feedback: none (no current persistence workflow)")


def _cleanup_preflight(
    db: Session,
    manifest: dict[str, Any],
    state: dict[str, Any],
) -> tuple[list[int], list[int], dict[str, list[int]]]:
    client_ids = list(state.get("client_ids", {}).values())
    report_ids = list(state.get("report_ids", {}).values())

    clients = db.scalars(select(Client).where(Client.id.in_(client_ids))).all()
    expected_clients = {
        state["client_ids"][entry["key"]]: entry for entry in manifest["clients"]
    }
    if any(
        client.id not in expected_clients
        or client.name != expected_clients[client.id]["name"]
        or client.contact_email != expected_clients[client.id]["contact_email"]
        for client in clients
    ):
        raise DatasetError("Cleanup refused: a client no longer matches its synthetic marker.")

    reports = db.scalars(select(Report).where(Report.id.in_(report_ids))).all()
    expected_reports = {
        state["report_ids"][entry["key"]]: entry for entry in manifest["reports"]
    }
    if any(
        report.id not in expected_reports
        or report.title != expected_reports[report.id]["title"]
        or report.source_filename != expected_reports[report.id]["source_filename"]
        or report.storage_path != expected_reports[report.id]["storage_path"]
        for report in reports
    ):
        raise DatasetError("Cleanup refused: a report no longer matches its synthetic marker.")

    table_ids: dict[str, list[int]] = {}
    table_models: dict[str, Any] = {
        "client_purchases": ClientPurchase,
        "market_metrics": MarketMetric,
        "ingestion_jobs": IngestionJob,
        "escalations": Escalation,
        "security_events": SecurityEvent,
    }
    for table_name, model in table_models.items():
        if table_name == "client_purchases":
            rows = db.scalars(
                select(model).where(
                    or_(
                        model.client_id.in_(client_ids),
                        model.report_id.in_(report_ids),
                    )
                )
            ).all()
            ids = [row.id for row in rows]
            if set(ids) != set(state["purchase_ids"]):
                raise DatasetError("Cleanup refused: purchase rows differ from the seed ledger.")
        elif table_name == "market_metrics":
            rows = db.scalars(
                select(model).where(model.report_id.in_(report_ids))
            ).all()
            if any(not row.is_synthetic for row in rows):
                raise DatasetError("Cleanup refused: a report has a non-synthetic metric.")
            ids = [row.id for row in rows]
            if set(ids) != set(state["metric_ids"]):
                raise DatasetError("Cleanup refused: metric rows differ from the seed ledger.")
        elif table_name == "ingestion_jobs":
            rows = db.scalars(
                select(model).where(model.report_id.in_(report_ids))
            ).all()
            ids = [row.id for row in rows]
            if not set(ids).issubset(set(state["ingestion_job_ids"])):
                raise DatasetError("Cleanup refused: untracked ingestion jobs exist.")
        elif table_name == "escalations":
            rows = db.scalars(
                select(model).where(model.client_id.in_(client_ids))
            ).all()
            ids = [row.id for row in rows]
            if not set(ids).issubset(set(state["escalation_ids"])):
                raise DatasetError("Cleanup refused: untracked client escalations exist.")
        else:
            rows = db.scalars(
                select(model).where(model.client_id.in_(client_ids))
            ).all()
            ids = [row.id for row in rows]
            if not set(ids).issubset(set(state["security_event_ids"])):
                raise DatasetError("Cleanup refused: untracked client security events exist.")
        table_ids[table_name] = ids

    for table_name, model in (
        ("conversations", Conversation),
        ("feedback", Feedback),
    ):
        count = db.scalar(
            select(func.count()).select_from(model).where(model.client_id.in_(client_ids))
        )
        if count:
            raise DatasetError(
                f"Cleanup refused: {table_name} contains rows not created by this dataset workflow."
            )
    message_count = db.scalar(
        select(func.count())
        .select_from(ConversationMessage)
        .join(Conversation, Conversation.id == ConversationMessage.conversation_id)
        .where(Conversation.client_id.in_(client_ids))
    )
    if message_count:
        raise DatasetError(
            "Cleanup refused: conversation messages exist for synthetic clients."
        )
    return client_ids, report_ids, table_ids


def cleanup_dataset() -> None:
    manifest = _load_manifest()
    state = _read_state()
    with SessionLocal() as db:
        _assert_target_database(db)
        client_ids, report_ids, table_ids = _cleanup_preflight(db, manifest, state)

        if VECTOR_STORE_PATH.exists() and report_ids:
            client = chromadb.PersistentClient(path=str(VECTOR_STORE_PATH))
            collection_names = {
                item.name if hasattr(item, "name") else str(item)
                for item in client.list_collections()
            }
            if COLLECTION_NAME in collection_names:
                collection = client.get_collection(name=COLLECTION_NAME)
                report_key_by_id = {
                    report_id: report_key
                    for report_key, report_id in state["report_ids"].items()
                }
                for report_id in report_ids:
                    result = collection.get(
                        where={"report_id": report_id},
                        include=["metadatas"],
                    )
                    chunk_ids = list(result["ids"])
                    expected_ids = set(
                        state["vector_ids"].get(report_key_by_id[report_id], [])
                    )
                    expected_ids.update(
                        f"report-{report_id}-chunk-{index}" for index in range(3)
                    )
                    if not set(chunk_ids).issubset(expected_ids):
                        raise DatasetError(
                            "Cleanup refused: unrecognized Chroma chunks exist for a synthetic report."
                        )
                    if chunk_ids:
                        collection.delete(ids=chunk_ids)
                if any(
                    collection.get(
                        where={"report_id": report_id},
                        include=["metadatas"],
                    )["ids"]
                    for report_id in report_ids
                ):
                    raise DatasetError("Chroma cleanup did not remove all synthetic report chunks.")

        for table_name, model in (
            ("security_events", SecurityEvent),
            ("escalations", Escalation),
            ("ingestion_jobs", IngestionJob),
            ("market_metrics", MarketMetric),
            ("client_purchases", ClientPurchase),
        ):
            for row_id in table_ids[table_name]:
                row = db.get(model, row_id)
                if row is not None:
                    db.delete(row)
        for report_id in report_ids:
            report = db.get(Report, report_id)
            if report is not None:
                db.delete(report)
        for client_id in client_ids:
            client = db.get(Client, client_id)
            if client is not None:
                db.delete(client)
        db.commit()

    with SessionLocal() as db:
        remaining = {
            "clients": db.scalar(
                select(func.count())
                .select_from(Client)
                .where(Client.id.in_(state["client_ids"].values()))
            ),
            "reports": db.scalar(
                select(func.count())
                .select_from(Report)
                .where(Report.id.in_(state["report_ids"].values()))
            ),
        }
        for table_name, model in (
            ("client_purchases", ClientPurchase),
            ("market_metrics", MarketMetric),
            ("ingestion_jobs", IngestionJob),
            ("escalations", Escalation),
            ("security_events", SecurityEvent),
        ):
            remaining[table_name] = db.scalar(
                select(func.count())
                .select_from(model)
                .where(model.id.in_(table_ids[table_name]))
            )
        remaining = {table_name: count for table_name, count in remaining.items() if count}
    if remaining:
        raise DatasetError("Cleanup completed partially; remaining row counts require review.")
    STATE_PATH.unlink(missing_ok=True)
    print("Removed only ledger-identified synthetic rows and Chroma chunks.")
    print("Synthetic Markdown fixtures and manifest were retained.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create, ingest, verify, or clean up the Project Atlas synthetic test dataset."
    )
    parser.add_argument(
        "operation",
        choices=("dry-run", "apply", "ingest", "exercise", "verify", "cleanup"),
    )
    operation = parser.parse_args().operation
    actions = {
        "dry-run": dry_run,
        "apply": apply_seed,
        "ingest": ingest_fixtures,
        "exercise": exercise_workflows,
        "verify": verify_dataset,
        "cleanup": cleanup_dataset,
    }
    try:
        actions[operation]()
    except DatasetError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1) from None
    except SQLAlchemyError:
        print(
            "ERROR: database operation failed; connection details were suppressed.",
            file=sys.stderr,
        )
        raise SystemExit(1) from None
    except Exception as error:
        print(
            f"ERROR: unexpected {type(error).__name__}; details were suppressed.",
            file=sys.stderr,
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()

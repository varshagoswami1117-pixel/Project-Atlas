from __future__ import annotations

import json
import logging
import math
import os
import re
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, TypedDict

import httpx
from chromadb.errors import ChromaError
from langgraph.graph import END, StateGraph
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from backend.database import (
    Escalation,
    MarketMetric,
    Report,
    SecurityEvent,
    SecurityEventSeverity,
)
from backend.ingestion import get_authorized_report_ids, retrieve_authorized_chunks

logger = logging.getLogger(__name__)

MAX_SQL_METRICS = 500
MAX_RETRIEVED_METRICS = 20
MAX_VECTOR_CHUNKS = 6
MAX_CITATIONS = 10


class WorkflowUnavailableError(RuntimeError):
    """Raised when a required retrieval backend cannot serve a request."""


class QuestionDomain(str, Enum):
    FACTUAL_LOOKUP = "Factual Lookup"
    COMPARISON = "Comparison"
    CALCULATION = "Calculation"
    OUT_OF_SCOPE = "Out of Scope"
    UNSAFE = "Unsafe"


class WorkflowState(TypedDict, total=False):
    db: Session
    readonly_db: Session
    client_id: int
    query: str
    domain: str
    routing_reason: str
    authorized_report_ids: list[int]
    metric_rows: list[dict[str, Any]]
    vector_chunks: list[dict[str, Any]]
    citations: list[dict[str, Any]]
    derived_values: list[dict[str, Any]]
    conflicts: list[dict[str, Any]]
    answer: str
    confidence: float
    escalated: bool
    escalation_id: int | None
    escalation_reason: str | None
    numeric_audit_passed: bool
    retrieval_error: str | None


_UNSAFE_PATTERNS = (
    re.compile(r"\bignore\b.{0,40}\b(previous|prior|system|developer)\b", re.I),
    re.compile(r"\b(reveal|print|show|exfiltrate)\b.{0,40}\b(system prompt|secret|password|api key|credential)s?\b", re.I),
    re.compile(r"\b(bypass|disable|evade)\b.{0,40}\b(auth|authorization|access control|security)\b", re.I),
    re.compile(r"\b(drop|delete|truncate|update|insert)\s+(table|from|into)\b", re.I),
    re.compile(r"\b(?:prompt injection|jailbreak|system override)\b", re.I),
)
_OUT_OF_SCOPE_PATTERNS = (
    re.compile(r"\b(other|another|all|every)\s+clients?\b", re.I),
    re.compile(r"\b(personal data|private data|contact details|email address|phone number)\b", re.I),
    re.compile(r"\b(political|election|politician|party politics)\b", re.I),
    re.compile(r"\b(weather|sports scores|medical diagnosis|recipe|celebrity gossip)\b", re.I),
)
_INJECTION_IN_SOURCE = re.compile(
    r"\b(ignore|disregard|override)\b.{0,80}\b(previous|prior|system|developer|instructions?)\b"
    r"|\b(?:NOTE TO AI SYSTEMS|system prompt|reveal secrets|bypass access)\b",
    re.I,
)
_TOKEN_PATTERN = re.compile(r"[a-zA-Z][a-zA-Z0-9_-]{2,}")
_STOP_WORDS = {
    "about",
    "across",
    "after",
    "among",
    "and",
    "are",
    "compare",
    "could",
    "from",
    "have",
    "into",
    "market",
    "please",
    "report",
    "that",
    "the",
    "their",
    "these",
    "this",
    "those",
    "what",
    "when",
    "where",
    "which",
    "with",
    "would",
}
_NUMBER_PATTERN = re.compile(r"(?<![A-Za-z])[-+]?\d[\d,]*(?:\.\d+)?%?(?![A-Za-z])")
_YEAR_PATTERN = re.compile(r"\b(?:19|20|21)\d{2}\b")


def _classify_question(query: str) -> tuple[QuestionDomain, str]:
    if any(pattern.search(query) for pattern in _UNSAFE_PATTERNS):
        return QuestionDomain.UNSAFE, "The request appears to seek unsafe access or override instructions."
    if any(pattern.search(query) for pattern in _OUT_OF_SCOPE_PATTERNS):
        return QuestionDomain.OUT_OF_SCOPE, "The request concerns personal, political, or unrelated information."
    if re.search(r"\b(cagr|compound annual|annualized growth|project(?:ion)?|forecast)\b", query, re.I):
        return QuestionDomain.CALCULATION, "The request asks for a calculation or projection."
    if re.search(r"\b(compare|comparison|versus|vs\.?|difference between)\b", query, re.I):
        return QuestionDomain.COMPARISON, "The request asks to compare source-backed information."
    return QuestionDomain.FACTUAL_LOOKUP, "The request is treated as a factual market-report lookup."


def _route_node(state: WorkflowState) -> dict[str, Any]:
    domain, reason = _classify_question(state["query"])
    return {"domain": domain.value, "routing_reason": reason}


def _word_tokens(value: str) -> set[str]:
    return {
        token.lower()
        for token in _TOKEN_PATTERN.findall(value)
        if token.lower() not in _STOP_WORDS
    }


def _source_contains_injection(text: str) -> bool:
    return _INJECTION_IN_SOURCE.search(text) is not None


def _log_injection_source(db: Session, client_id: int, metadata: dict[str, Any]) -> None:
    db.add(
        SecurityEvent(
            client_id=client_id,
            event_type="document_prompt_injection_detected",
            severity=SecurityEventSeverity.WARNING,
            resource_type="report",
            resource_id=str(metadata.get("report_id", "")),
            details={
                "report_id": metadata.get("report_id"),
                "page": metadata.get("page"),
                "action": "excluded_from_agent_context",
            },
        )
    )


def _metric_record(row: Any) -> dict[str, Any]:
    metric, report_title = row
    return {
        "id": metric.id,
        "report_id": metric.report_id,
        "report_title": report_title,
        "metric_name": metric.metric_name,
        "value": None if metric.metric_value is None else str(metric.metric_value),
        "unit": metric.unit,
        "geography": metric.geography,
        "segment": metric.segment,
        "period_start": None if metric.period_start is None else metric.period_start.isoformat(),
        "period_end": None if metric.period_end is None else metric.period_end.isoformat(),
        "source_page": metric.source_page,
        "source_quote": metric.source_quote,
    }


def _include_synthetic_metrics() -> bool:
    configured = os.getenv("ATLAS_INCLUDE_SYNTHETIC_METRICS", "false").strip().casefold()
    if configured not in {"true", "false"}:
        raise RuntimeError("ATLAS_INCLUDE_SYNTHETIC_METRICS must be true or false.")
    return configured == "true"


def _metric_statement(authorized_ids: list[int]):
    statement = (
        select(MarketMetric, Report.title)
        .join(Report, Report.id == MarketMetric.report_id)
        .where(MarketMetric.report_id.in_(authorized_ids))
    )
    if not _include_synthetic_metrics():
        statement = statement.where(MarketMetric.is_synthetic.is_(False))
    return (
        statement.order_by(
            MarketMetric.report_id,
            MarketMetric.period_start,
            MarketMetric.id,
        )
        .limit(MAX_SQL_METRICS)
    )


def _metric_year(metric: dict[str, Any]) -> int | None:
    for key in ("period_end", "period_start"):
        value = metric.get(key)
        if value:
            return int(value[:4])
    return None


def _structure_key(metric: dict[str, Any]) -> tuple[str, ...]:
    return (
        str(metric.get("metric_name") or "").strip().casefold(),
        str(metric.get("unit") or "").strip().casefold(),
        str(metric.get("geography") or "").strip().casefold(),
        str(metric.get("segment") or "").strip().casefold(),
        str(metric.get("period_start") or ""),
        str(metric.get("period_end") or ""),
    )


def _detect_conflicts(metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for metric in metrics:
        if metric["value"] is not None:
            grouped.setdefault(_structure_key(metric), []).append(metric)

    conflicts: list[dict[str, Any]] = []
    for key, records in grouped.items():
        distinct_values = {str(Decimal(record["value"])) for record in records}
        distinct_reports = {record["report_id"] for record in records}
        if len(distinct_values) < 2 or len(distinct_reports) < 2:
            continue
        conflicts.append(
            {
                "metric_name": key[0],
                "unit": key[1],
                "geography": key[2],
                "segment": key[3],
                "period_start": key[4] or None,
                "period_end": key[5] or None,
                "sources": [
                    {
                        "report_id": record["report_id"],
                        "value": record["value"],
                        "quote": record["source_quote"],
                        "page": record.get("source_page"),
                    }
                    for record in records
                ],
            }
        )
    return conflicts


def _retrieval_node(state: WorkflowState) -> dict[str, Any]:
    db = state["db"]
    readonly_db = state["readonly_db"]
    client_id = state["client_id"]
    query = state["query"]
    try:
        authorized_ids = get_authorized_report_ids(readonly_db, client_id)
    except SQLAlchemyError as error:
        logger.exception("Could not load report entitlements for client_id=%s", client_id)
        raise WorkflowUnavailableError("Entitlement lookup failed.") from error
    if not authorized_ids:
        return {
            "authorized_report_ids": [],
            "metric_rows": [],
            "vector_chunks": [],
            "citations": [],
            "conflicts": [],
        }

    try:
        metrics = [
            _metric_record(row)
            for row in readonly_db.execute(_metric_statement(authorized_ids)).all()
        ]
    except SQLAlchemyError as error:
        logger.exception("Could not query authorized metrics for client_id=%s", client_id)
        raise WorkflowUnavailableError("Metric retrieval failed.") from error
    query_tokens = _word_tokens(query)
    if query_tokens:
        ranked_metrics = sorted(
            (
                (
                    len(
                        query_tokens
                        & _word_tokens(
                            " ".join(
                                str(metric.get(field) or "")
                                for field in (
                                    "metric_name",
                                    "unit",
                                    "geography",
                                    "segment",
                                    "report_title",
                                    "source_quote",
                                )
                            )
                        )
                    ),
                    index,
                    metric,
                )
                for index, metric in enumerate(metrics)
            ),
            key=lambda item: (-item[0], item[1]),
        )
        metrics = [metric for score, _, metric in ranked_metrics if score > 0][
            :MAX_RETRIEVED_METRICS
        ]
    else:
        metrics = metrics[:MAX_RETRIEVED_METRICS]

    try:
        vector_results = retrieve_authorized_chunks(
            query,
            authorized_ids,
            limit=MAX_VECTOR_CHUNKS,
        )
    except (ChromaError, httpx.HTTPError, OSError) as error:
        logger.exception("Vector retrieval failed for client_id=%s", client_id)
        raise WorkflowUnavailableError("Vector retrieval failed.") from error
    safe_chunks: list[dict[str, Any]] = []
    for chunk in vector_results:
        if _source_contains_injection(chunk["text"]):
            _log_injection_source(db, client_id, chunk["metadata"])
            continue
        safe_chunks.append(chunk)

    safe_metrics: list[dict[str, Any]] = []
    for metric in metrics:
        quote = metric.get("source_quote")
        if quote and _source_contains_injection(quote):
            _log_injection_source(
                db,
                client_id,
                {"report_id": metric["report_id"], "page": metric["source_page"]},
            )
            continue
        safe_metrics.append(metric)

    citations: list[dict[str, Any]] = []
    seen_citations: set[tuple[int, int | None, str]] = set()

    for metric in safe_metrics:
        quote = metric.get("source_quote")
        if not quote:
            continue
        key = (metric["report_id"], metric.get("source_page"), quote)
        if key in seen_citations:
            continue
        seen_citations.add(key)
        citations.append(
            {
                "citation_id": f"c{len(citations) + 1}",
                "report_id": metric["report_id"],
                "report_title": metric["report_title"],
                "page": metric.get("source_page"),
                "quote": quote[:1200],
                "source": "market_metrics",
            }
        )

    for chunk in safe_chunks:
        metadata = chunk["metadata"]
        key = (metadata["report_id"], metadata["page"], chunk["text"])
        if key in seen_citations:
            continue
        seen_citations.add(key)
        citations.append(
            {
                "citation_id": f"c{len(citations) + 1}",
                "report_id": metadata["report_id"],
                "report_title": None,
                "page": metadata["page"],
                "quote": chunk["text"][:1200],
                "source": "vector_chunk",
            }
        )

    return {
        "authorized_report_ids": authorized_ids,
        "metric_rows": safe_metrics,
        "vector_chunks": safe_chunks,
        "citations": citations[:MAX_CITATIONS],
        "conflicts": _detect_conflicts(safe_metrics),
    }


def _select_calculation_series(
    metrics: list[dict[str, Any]],
    years: list[int],
    projection: bool,
) -> tuple[list[dict[str, Any]], int] | None:
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for metric in metrics:
        year = _metric_year(metric)
        quote = metric.get("source_quote")
        value = metric.get("value")
        if year is None or not quote or value is None:
            continue
        try:
            if Decimal(value) <= 0:
                continue
        except InvalidOperation:
            continue
        grouped.setdefault(_structure_key(metric)[:4], []).append(metric)

    candidates: list[tuple[list[dict[str, Any]], int]] = []
    if projection and years:
        target_year = years[-1]
        for records in grouped.values():
            eligible = sorted(
                (record for record in records if (_metric_year(record) or 0) < target_year),
                key=lambda record: _metric_year(record) or 0,
            )
            if len(eligible) >= 2:
                start, end = eligible[-2], eligible[-1]
                delta = (_metric_year(end) or 0) - (_metric_year(start) or 0)
                if delta > 0:
                    candidates.append(([start, end], target_year))
    elif len(years) >= 2:
        start_year, end_year = years[0], years[1]
        for records in grouped.values():
            by_year = {_metric_year(record): record for record in records}
            if start_year in by_year and end_year in by_year and end_year > start_year:
                candidates.append(([by_year[start_year], by_year[end_year]], end_year))

    if len(candidates) != 1:
        return None
    return candidates[0]


def _calculator_node(state: WorkflowState) -> dict[str, Any]:
    if state.get("conflicts"):
        return {
            "derived_values": [],
            "calculation_note": (
                "Conflicting source records prevent a reliable calculation; no value was guessed."
            ),
        }

    query = state["query"]
    is_projection = re.search(r"\b(project(?:ion)?|forecast)\b", query, re.I) is not None
    years = [int(year) for year in _YEAR_PATTERN.findall(query)]
    selected = _select_calculation_series(state.get("metric_rows", []), years, is_projection)
    if selected is None:
        return {
            "derived_values": [],
            "calculation_note": (
                "No unique, citation-backed positive-value series with the requested periods "
                "was available; no calculation was guessed."
            ),
        }

    records, target_year = selected
    start, end = records
    start_year = _metric_year(start)
    end_year = _metric_year(end)
    if start_year is None or end_year is None or end_year <= start_year:
        return {"derived_values": [], "calculation_note": "The source periods are not usable for CAGR."}

    start_value = Decimal(start["value"])
    end_value = Decimal(end["value"])
    years_elapsed = end_year - start_year
    cagr = Decimal(str(math.pow(float(end_value / start_value), 1.0 / years_elapsed) - 1.0))
    derived: dict[str, Any] = {
        "name": "CAGR",
        "value": f"{(cagr * 100).quantize(Decimal('0.01'))}%",
        "formula": "CAGR = ((ending_value / starting_value) ** (1 / elapsed_years)) - 1",
        "assumptions": [
            "Both source values describe the same metric, unit, geography, and segment.",
            "The source periods are treated as annual observations.",
            "The starting and ending values are positive and use the same basis.",
        ],
        "inputs": [
            {
                "value": str(start_value),
                "year": start_year,
                "citation_id": _citation_id_for_metric(start, state.get("citations", [])),
            },
            {
                "value": str(end_value),
                "year": end_year,
                "citation_id": _citation_id_for_metric(end, state.get("citations", [])),
            },
        ],
    }

    if is_projection:
        if target_year <= end_year:
            return {
                "derived_values": [],
                "calculation_note": "The requested projection year must be later than the latest source year.",
            }
        projected = end_value * Decimal(str(math.pow(float(1 + cagr), target_year - end_year)))
        derived["projection"] = {
            "year": target_year,
            "value": str(projected.quantize(Decimal("0.01"))),
            "formula": "future_value = latest_value * (1 + CAGR) ** projection_years",
            "assumptions": [
                "The historical CAGR remains constant through the projection year.",
                "No inflation, currency, or market-regime adjustment is applied.",
            ],
        }

    if any(input_value["citation_id"] is None for input_value in derived["inputs"]):
        return {
            "derived_values": [],
            "calculation_note": "The source values could not be tied to stored citations; no calculation was returned.",
        }
    return {"derived_values": [derived], "calculation_note": None}


def _citation_id_for_metric(
    metric: dict[str, Any],
    citations: list[dict[str, Any]],
) -> str | None:
    for citation in citations:
        if (
            citation["source"] == "market_metrics"
            and citation["report_id"] == metric["report_id"]
            and citation["page"] == metric.get("source_page")
            and citation["quote"] == metric.get("source_quote", "")[:1200]
        ):
            return citation["citation_id"]
    return None


def _writer_node(state: WorkflowState) -> dict[str, Any]:
    domain = state["domain"]
    if domain == QuestionDomain.UNSAFE.value:
        return {
            "answer": "I can’t help bypass safeguards, reveal secrets, or execute unsafe instructions.",
            "citations": [],
            "confidence": 1.0,
        }
    if domain == QuestionDomain.OUT_OF_SCOPE.value:
        return {
            "answer": (
                "I can help with questions grounded in authorized market reports, but not "
                "personal/client records, political topics, or unrelated requests."
            ),
            "citations": [],
            "confidence": 1.0,
        }

    citations = state.get("citations", [])
    derived_values = state.get("derived_values", [])
    conflicts = state.get("conflicts", [])
    if domain == QuestionDomain.CALCULATION.value:
        if derived_values:
            answer = (
                "The calculation is based on the cited source values. The derived_values "
                "field contains the formula, result, source inputs, and assumptions."
            )
        else:
            answer = state.get(
                "calculation_note",
                "I could not calculate this without unique, citation-backed source values and periods.",
            )
    elif not citations:
        answer = "I could not find citation-backed evidence in reports this client is authorized to access."
    else:
        preface = (
            "The authorized reports contain conflicting values for the same metric and period. "
            "Relevant source excerpts follow:"
            if conflicts
            else "Relevant excerpts from the authorized reports:"
        )
        answer = preface + "\n" + "\n".join(
            f'- "{citation["quote"]}"' for citation in citations[:6]
        )
    return {"answer": answer, "citations": citations}


def _normalized_numbers(value: str) -> set[str]:
    return {match.replace(",", "").rstrip("%") for match in _NUMBER_PATTERN.findall(value)}


def _critic_node(state: WorkflowState) -> dict[str, Any]:
    citations = state.get("citations", [])
    answer = state.get("answer", "")
    derived_values = state.get("derived_values", [])
    if state.get("domain") in {
        QuestionDomain.UNSAFE.value,
        QuestionDomain.OUT_OF_SCOPE.value,
    }:
        return {
            "confidence": 1.0,
            "numeric_audit_passed": True,
        }

    citation_numbers = set().union(
        *(_normalized_numbers(citation["quote"]) for citation in citations)
    ) if citations else set()
    answer_numbers = _normalized_numbers(answer)
    numeric_audit_passed = answer_numbers <= citation_numbers

    citation_ids = {citation["citation_id"] for citation in citations}
    derived_inputs_valid = all(
        item.get("citation_id") in citation_ids
        for derived in derived_values
        for item in derived.get("inputs", [])
    )
    if derived_values and not derived_inputs_valid:
        numeric_audit_passed = False

    conflicts = state.get("conflicts", [])
    if conflicts:
        confidence = 0.45
    elif not citations:
        confidence = 0.25
    elif not numeric_audit_passed:
        confidence = 0.2
    elif derived_values:
        confidence = 0.85
    else:
        confidence = min(0.9, 0.72 + 0.03 * min(len(citations) - 1, 6))

    if not numeric_audit_passed:
        answer = "I could not verify every numerical figure against the available source citations."
        derived_values = []

    return {
        "answer": answer,
        "derived_values": derived_values,
        "confidence": confidence,
        "numeric_audit_passed": numeric_audit_passed,
    }


def _minimum_confidence() -> float:
    configured = os.getenv("ATLAS_MIN_CONFIDENCE", "0.65")
    try:
        threshold = float(configured)
    except ValueError as error:
        raise RuntimeError("ATLAS_MIN_CONFIDENCE must be a number between 0 and 1.") from error
    if not 0.0 <= threshold <= 1.0:
        raise RuntimeError("ATLAS_MIN_CONFIDENCE must be a number between 0 and 1.")
    return threshold


def _escalation_node(state: WorkflowState) -> dict[str, Any]:
    conflicts = state.get("conflicts", [])
    confidence = state["confidence"]
    reason = "Conflicting source metrics" if conflicts else "Low answer confidence"
    details = {
        "domain": state.get("domain"),
        "confidence": confidence,
        "minimum_confidence": _minimum_confidence(),
        "conflicts": conflicts,
        "numeric_audit_passed": state.get("numeric_audit_passed", False),
    }
    escalation = Escalation(
        client_id=state["client_id"],
        reason=reason,
        details=json.dumps(details, ensure_ascii=False, default=str),
    )
    db = state["db"]
    db.add(escalation)
    db.commit()
    db.refresh(escalation)
    logger.warning(
        "Escalated report copilot answer client_id=%s escalation_id=%s reason=%s",
        state["client_id"],
        escalation.id,
        reason,
    )
    return {
        "escalated": True,
        "escalation_id": escalation.id,
        "escalation_reason": reason,
    }


def _route_after_classification(state: WorkflowState) -> str:
    if state["domain"] in {QuestionDomain.UNSAFE.value, QuestionDomain.OUT_OF_SCOPE.value}:
        return "writer"
    return "retriever"


def _route_after_retrieval(state: WorkflowState) -> str:
    if state["domain"] == QuestionDomain.CALCULATION.value:
        return "calculator"
    return "writer"


def _route_after_critic(state: WorkflowState) -> str:
    if state.get("domain") in {QuestionDomain.UNSAFE.value, QuestionDomain.OUT_OF_SCOPE.value}:
        return END
    if state.get("conflicts") or state.get("confidence", 0.0) < _minimum_confidence():
        return "escalation"
    return END


def _build_workflow():
    workflow = StateGraph(WorkflowState)
    workflow.add_node("router", _route_node)
    workflow.add_node("retriever", _retrieval_node)
    workflow.add_node("calculator", _calculator_node)
    workflow.add_node("writer", _writer_node)
    workflow.add_node("critic", _critic_node)
    workflow.add_node("escalation_guard", _escalation_node)
    workflow.set_entry_point("router")
    workflow.add_conditional_edges(
        "router",
        _route_after_classification,
        {"retriever": "retriever", "writer": "writer"},
    )
    workflow.add_conditional_edges(
        "retriever",
        _route_after_retrieval,
        {"calculator": "calculator", "writer": "writer"},
    )
    workflow.add_edge("calculator", "writer")
    workflow.add_edge("writer", "critic")
    workflow.add_conditional_edges(
        "critic",
        _route_after_critic,
        {"escalation": "escalation_guard", END: END},
    )
    workflow.add_edge("escalation_guard", END)
    return workflow.compile()


_REPORT_COPILOT_GRAPH = _build_workflow()


def run_report_copilot(
    db: Session,
    readonly_db: Session,
    client_id: int,
    query: str,
) -> dict[str, Any]:
    cleaned_query = query.strip()
    if not cleaned_query:
        raise ValueError("query cannot be blank.")
    state = _REPORT_COPILOT_GRAPH.invoke(
        {
            "db": db,
            "readonly_db": readonly_db,
            "client_id": client_id,
            "query": cleaned_query,
            "escalated": False,
            "escalation_id": None,
            "derived_values": [],
            "conflicts": [],
            "citations": [],
        }
    )
    return {
        "answer": state["answer"],
        "citations": state.get("citations", []),
        "confidence": state.get("confidence", 0.0),
        "conflicts": state.get("conflicts", []),
        "derived_values": state.get("derived_values", []),
        "escalated": state.get("escalated", False),
    }

from __future__ import annotations

import os
from datetime import date, datetime
from decimal import Decimal
from enum import Enum as PythonEnum
from pathlib import Path
from typing import Any, Generator

from dotenv import load_dotenv
from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    Date,
    Enum,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    TIMESTAMP,
    UniqueConstraint,
    create_engine,
    func,
    text,
)
from sqlalchemy.dialects.mysql import BIGINT, TINYINT
from sqlalchemy.engine import URL
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship, sessionmaker

load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env", override=False)


def _required_setting(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise RuntimeError(f"Required database setting {name} is missing or empty.")
    return value.strip()


def _positive_int_setting(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        parsed_value = int(value)
    except ValueError as error:
        raise RuntimeError(f"Database setting {name} must be an integer.") from error
    if parsed_value <= 0:
        raise RuntimeError(f"Database setting {name} must be greater than zero.")
    return parsed_value


def _non_negative_int_setting(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        parsed_value = int(value)
    except ValueError as error:
        raise RuntimeError(f"Database setting {name} must be an integer.") from error
    if parsed_value < 0:
        raise RuntimeError(f"Database setting {name} cannot be negative.")
    return parsed_value


def _make_database_url() -> URL:
    port = _positive_int_setting("DB_PORT", 3307)
    return URL.create(
        drivername="mysql+pymysql",
        username=_required_setting("DB_USER"),
        password=_required_setting("DB_PASSWORD"),
        host=_required_setting("DB_HOST"),
        port=port,
        database=_required_setting("DB_NAME"),
        query={"charset": "utf8mb4"},
    )


engine = create_engine(
    _make_database_url(),
    pool_pre_ping=True,
    pool_recycle=_positive_int_setting("DB_POOL_RECYCLE", 1800),
    pool_size=_positive_int_setting("DB_POOL_SIZE", 5),
    max_overflow=_non_negative_int_setting("DB_MAX_OVERFLOW", 10),
    pool_timeout=_positive_int_setting("DB_POOL_TIMEOUT", 30),
    connect_args={
        "connect_timeout": _positive_int_setting("DB_CONNECT_TIMEOUT", 10),
        "read_timeout": _positive_int_setting("DB_READ_TIMEOUT", 30),
        "write_timeout": _positive_int_setting("DB_WRITE_TIMEOUT", 30),
    },
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


class ClientStatus(str, PythonEnum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    SUSPENDED = "suspended"


class DataClassification(str, PythonEnum):
    PUBLIC = "public"
    LICENSED = "licensed"
    CONFIDENTIAL = "confidential"


class IngestionStatus(str, PythonEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class PurchaseStatus(str, PythonEnum):
    ACTIVE = "active"
    EXPIRED = "expired"
    REVOKED = "revoked"


class MessageRole(str, PythonEnum):
    SYSTEM = "system"
    USER = "user"
    ASSISTANT = "assistant"
    TOOL = "tool"


class EscalationStatus(str, PythonEnum):
    OPEN = "open"
    IN_REVIEW = "in_review"
    RESOLVED = "resolved"
    CLOSED = "closed"


class SecurityEventSeverity(str, PythonEnum):
    INFO = "info"
    WARNING = "warning"
    HIGH = "high"
    CRITICAL = "critical"


class IngestionJobStatus(str, PythonEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


def _mysql_enum(enum_type: type[PythonEnum], name: str) -> Enum:
    return Enum(
        enum_type,
        name=name,
        native_enum=True,
        validate_strings=True,
        values_callable=lambda members: [member.value for member in members],
    )


class Client(Base):
    __tablename__ = "clients"

    id: Mapped[int] = mapped_column(BIGINT(unsigned=True), primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    contact_email: Mapped[str | None] = mapped_column(String(254), unique=True)
    status: Mapped[ClientStatus] = mapped_column(
        _mysql_enum(ClientStatus, "clientstatus"),
        nullable=False,
        default=ClientStatus.ACTIVE,
        server_default=text("'active'"),
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=func.current_timestamp()
    )

    purchases: Mapped[list[ClientPurchase]] = relationship(back_populates="client")
    conversations: Mapped[list[Conversation]] = relationship(back_populates="client")
    feedback_entries: Mapped[list[Feedback]] = relationship(back_populates="client")
    escalations: Mapped[list[Escalation]] = relationship(back_populates="client")
    security_events: Mapped[list[SecurityEvent]] = relationship(back_populates="client")


class Report(Base):
    __tablename__ = "reports"
    __table_args__ = (
        Index("idx_reports_status", "ingestion_status"),
        Index("idx_reports_publication_date", "publication_date"),
    )

    id: Mapped[int] = mapped_column(BIGINT(unsigned=True), primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    source_filename: Mapped[str] = mapped_column(String(255), nullable=False)
    storage_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    report_type: Mapped[str | None] = mapped_column(String(100))
    publisher: Mapped[str | None] = mapped_column(String(200))
    publication_date: Mapped[date | None] = mapped_column(Date)
    data_classification: Mapped[DataClassification] = mapped_column(
        _mysql_enum(DataClassification, "dataclassification"),
        nullable=False,
        default=DataClassification.LICENSED,
        server_default=text("'licensed'"),
    )
    checksum_sha256: Mapped[str | None] = mapped_column(String(64))
    ingestion_status: Mapped[IngestionStatus] = mapped_column(
        _mysql_enum(IngestionStatus, "ingestionstatus"),
        nullable=False,
        default=IngestionStatus.PENDING,
        server_default=text("'pending'"),
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=func.current_timestamp()
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
        server_onupdate=func.current_timestamp(),
    )

    purchases: Mapped[list[ClientPurchase]] = relationship(back_populates="report")
    metrics: Mapped[list[MarketMetric]] = relationship(back_populates="report")
    ingestion_jobs: Mapped[list[IngestionJob]] = relationship(back_populates="report")


class ClientPurchase(Base):
    __tablename__ = "client_purchases"
    __table_args__ = (
        UniqueConstraint("client_id", "report_id", name="uq_client_report"),
        Index("idx_purchase_access", "client_id", "report_id", "status"),
    )

    id: Mapped[int] = mapped_column(BIGINT(unsigned=True), primary_key=True, autoincrement=True)
    client_id: Mapped[int] = mapped_column(
        BIGINT(unsigned=True), ForeignKey("clients.id"), nullable=False
    )
    report_id: Mapped[int] = mapped_column(
        BIGINT(unsigned=True), ForeignKey("reports.id"), nullable=False
    )
    purchase_reference: Mapped[str | None] = mapped_column(String(150), unique=True)
    purchased_at: Mapped[datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=func.current_timestamp()
    )
    expires_at: Mapped[datetime | None] = mapped_column(TIMESTAMP)
    status: Mapped[PurchaseStatus] = mapped_column(
        _mysql_enum(PurchaseStatus, "purchasestatus"),
        nullable=False,
        default=PurchaseStatus.ACTIVE,
        server_default=text("'active'"),
    )

    client: Mapped[Client] = relationship(back_populates="purchases")
    report: Mapped[Report] = relationship(back_populates="purchases")


class MarketMetric(Base):
    __tablename__ = "market_metrics"
    __table_args__ = (
        Index("idx_metrics_report", "report_id"),
        Index("idx_metrics_name", "metric_name"),
        Index("idx_metrics_geography", "geography"),
    )

    id: Mapped[int] = mapped_column(BIGINT(unsigned=True), primary_key=True, autoincrement=True)
    report_id: Mapped[int] = mapped_column(
        BIGINT(unsigned=True), ForeignKey("reports.id"), nullable=False
    )
    metric_name: Mapped[str] = mapped_column(String(200), nullable=False)
    metric_value: Mapped[Decimal | None] = mapped_column(Numeric(24, 6))
    unit: Mapped[str | None] = mapped_column(String(80))
    geography: Mapped[str | None] = mapped_column(String(150))
    segment: Mapped[str | None] = mapped_column(String(150))
    period_start: Mapped[date | None] = mapped_column(Date)
    period_end: Mapped[date | None] = mapped_column(Date)
    source_page: Mapped[int | None] = mapped_column(Integer)
    source_quote: Mapped[str | None] = mapped_column(Text)
    is_synthetic: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=text("0")
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=func.current_timestamp()
    )

    report: Mapped[Report] = relationship(back_populates="metrics")


class Conversation(Base):
    __tablename__ = "conversations"
    __table_args__ = (Index("idx_conversations_client", "client_id", "updated_at"),)

    id: Mapped[int] = mapped_column(BIGINT(unsigned=True), primary_key=True, autoincrement=True)
    client_id: Mapped[int] = mapped_column(
        BIGINT(unsigned=True), ForeignKey("clients.id"), nullable=False
    )
    title: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=func.current_timestamp()
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
        server_onupdate=func.current_timestamp(),
    )

    client: Mapped[Client] = relationship(back_populates="conversations")
    messages: Mapped[list[ConversationMessage]] = relationship(back_populates="conversation")
    feedback_entries: Mapped[list[Feedback]] = relationship(back_populates="conversation")
    escalations: Mapped[list[Escalation]] = relationship(back_populates="conversation")


class ConversationMessage(Base):
    __tablename__ = "conversation_messages"
    __table_args__ = (Index("idx_messages_conversation", "conversation_id", "created_at"),)

    id: Mapped[int] = mapped_column(BIGINT(unsigned=True), primary_key=True, autoincrement=True)
    conversation_id: Mapped[int] = mapped_column(
        BIGINT(unsigned=True), ForeignKey("conversations.id"), nullable=False
    )
    role: Mapped[MessageRole] = mapped_column(
        _mysql_enum(MessageRole, "messagerole"), nullable=False
    )
    content: Mapped[str] = mapped_column(Text, nullable=False)
    citations: Mapped[Any | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=func.current_timestamp()
    )

    conversation: Mapped[Conversation] = relationship(back_populates="messages")
    feedback_entries: Mapped[list[Feedback]] = relationship(
        back_populates="message", foreign_keys="Feedback.message_id"
    )
    escalations: Mapped[list[Escalation]] = relationship(
        back_populates="message", foreign_keys="Escalation.message_id"
    )


class Feedback(Base):
    __tablename__ = "feedback"
    __table_args__ = (
        CheckConstraint("rating IS NULL OR rating BETWEEN 1 AND 5", name="chk_feedback_rating"),
        Index("idx_feedback_client", "client_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(BIGINT(unsigned=True), primary_key=True, autoincrement=True)
    conversation_id: Mapped[int] = mapped_column(
        BIGINT(unsigned=True), ForeignKey("conversations.id"), nullable=False
    )
    message_id: Mapped[int | None] = mapped_column(
        BIGINT(unsigned=True), ForeignKey("conversation_messages.id")
    )
    client_id: Mapped[int] = mapped_column(
        BIGINT(unsigned=True), ForeignKey("clients.id"), nullable=False
    )
    rating: Mapped[int | None] = mapped_column(TINYINT(unsigned=True))
    feedback_text: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=func.current_timestamp()
    )

    conversation: Mapped[Conversation] = relationship(back_populates="feedback_entries")
    message: Mapped[ConversationMessage | None] = relationship(
        back_populates="feedback_entries", foreign_keys=[message_id]
    )
    client: Mapped[Client] = relationship(back_populates="feedback_entries")


class Escalation(Base):
    __tablename__ = "escalations"
    __table_args__ = (
        Index("idx_escalations_status", "status", "created_at"),
        Index("idx_escalations_client", "client_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(BIGINT(unsigned=True), primary_key=True, autoincrement=True)
    client_id: Mapped[int] = mapped_column(
        BIGINT(unsigned=True), ForeignKey("clients.id"), nullable=False
    )
    conversation_id: Mapped[int | None] = mapped_column(
        BIGINT(unsigned=True), ForeignKey("conversations.id")
    )
    message_id: Mapped[int | None] = mapped_column(
        BIGINT(unsigned=True), ForeignKey("conversation_messages.id")
    )
    reason: Mapped[str] = mapped_column(String(255), nullable=False)
    details: Mapped[str | None] = mapped_column(Text)
    status: Mapped[EscalationStatus] = mapped_column(
        _mysql_enum(EscalationStatus, "escalationstatus"),
        nullable=False,
        default=EscalationStatus.OPEN,
        server_default=text("'open'"),
    )
    assigned_to: Mapped[str | None] = mapped_column(String(200))
    resolution_notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=func.current_timestamp()
    )
    resolved_at: Mapped[datetime | None] = mapped_column(TIMESTAMP)

    client: Mapped[Client] = relationship(back_populates="escalations")
    conversation: Mapped[Conversation | None] = relationship(back_populates="escalations")
    message: Mapped[ConversationMessage | None] = relationship(
        back_populates="escalations", foreign_keys=[message_id]
    )


class SecurityEvent(Base):
    __tablename__ = "security_events"
    __table_args__ = (
        Index("idx_security_event_type", "event_type", "created_at"),
        Index("idx_security_severity", "severity", "created_at"),
    )

    id: Mapped[int] = mapped_column(BIGINT(unsigned=True), primary_key=True, autoincrement=True)
    client_id: Mapped[int | None] = mapped_column(
        BIGINT(unsigned=True), ForeignKey("clients.id")
    )
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    severity: Mapped[SecurityEventSeverity] = mapped_column(
        _mysql_enum(SecurityEventSeverity, "securityeventseverity"),
        nullable=False,
        default=SecurityEventSeverity.INFO,
        server_default=text("'info'"),
    )
    resource_type: Mapped[str | None] = mapped_column(String(100))
    resource_id: Mapped[str | None] = mapped_column(String(150))
    details: Mapped[Any | None] = mapped_column(JSON)
    request_id: Mapped[str | None] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=func.current_timestamp()
    )

    client: Mapped[Client | None] = relationship(back_populates="security_events")


class IngestionJob(Base):
    __tablename__ = "ingestion_jobs"
    __table_args__ = (
        Index("idx_ingestion_status", "status", "created_at"),
        Index("idx_ingestion_report", "report_id"),
    )

    id: Mapped[int] = mapped_column(BIGINT(unsigned=True), primary_key=True, autoincrement=True)
    report_id: Mapped[int] = mapped_column(
        BIGINT(unsigned=True), ForeignKey("reports.id"), nullable=False
    )
    status: Mapped[IngestionJobStatus] = mapped_column(
        _mysql_enum(IngestionJobStatus, "ingestionjobstatus"),
        nullable=False,
        default=IngestionJobStatus.QUEUED,
        server_default=text("'queued'"),
    )
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    started_at: Mapped[datetime | None] = mapped_column(TIMESTAMP)
    finished_at: Mapped[datetime | None] = mapped_column(TIMESTAMP)
    error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=func.current_timestamp()
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
        server_onupdate=func.current_timestamp(),
    )

    report: Mapped[Report] = relationship(back_populates="ingestion_jobs")


def get_db() -> Generator[Session, None, None]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def dispose_db() -> None:
    engine.dispose()

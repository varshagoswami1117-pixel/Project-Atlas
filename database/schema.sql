
-- Project Atlas: Report Copilot
-- MySQL 8 starter database schema
-- This file defines tables only. It does not insert sample data.

CREATE DATABASE IF NOT EXISTS atlas_copilot
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;

USE atlas_copilot;

-- 1. Clients: organizations or customers using the copilot
CREATE TABLE IF NOT EXISTS clients (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    contact_email VARCHAR(254) UNIQUE,
    status ENUM('active', 'inactive', 'suspended')
        NOT NULL DEFAULT 'active',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- 2. Reports: uploaded and ingested PDF reports
CREATE TABLE IF NOT EXISTS reports (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    source_filename VARCHAR(255) NOT NULL,
    storage_path VARCHAR(1024) NOT NULL,
    report_type VARCHAR(100),
    publisher VARCHAR(200),
    publication_date DATE,
    data_classification ENUM('public', 'licensed', 'confidential')
        NOT NULL DEFAULT 'licensed',
    checksum_sha256 CHAR(64),
    ingestion_status ENUM(
        'pending', 'processing', 'completed', 'failed'
    ) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_reports_status (ingestion_status),
    INDEX idx_reports_publication_date (publication_date)
) ENGINE=InnoDB;

-- 3. Client purchases and report entitlements
-- Access is granted per client and per report.
CREATE TABLE IF NOT EXISTS client_purchases (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    client_id BIGINT UNSIGNED NOT NULL,
    report_id BIGINT UNSIGNED NOT NULL,
    purchase_reference VARCHAR(150) UNIQUE,
    purchased_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NULL,
    status ENUM('active', 'expired', 'revoked')
        NOT NULL DEFAULT 'active',
    FOREIGN KEY (client_id) REFERENCES clients(id),
    FOREIGN KEY (report_id) REFERENCES reports(id),
    UNIQUE KEY uq_client_report (client_id, report_id),
    INDEX idx_purchase_access (client_id, report_id, status)
) ENGINE=InnoDB;

-- 4. Market metrics extracted from reports
CREATE TABLE IF NOT EXISTS market_metrics (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    report_id BIGINT UNSIGNED NOT NULL,
    metric_name VARCHAR(200) NOT NULL,
    metric_value DECIMAL(24, 6),
    unit VARCHAR(80),
    geography VARCHAR(150),
    segment VARCHAR(150),
    period_start DATE,
    period_end DATE,
    source_page INT UNSIGNED,
    source_quote TEXT,
    is_synthetic BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (report_id) REFERENCES reports(id),
    INDEX idx_metrics_report (report_id),
    INDEX idx_metrics_name (metric_name),
    INDEX idx_metrics_geography (geography)
) ENGINE=InnoDB;

-- 5. Conversations: chat sessions belonging to a client
CREATE TABLE IF NOT EXISTS conversations (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    client_id BIGINT UNSIGNED NOT NULL,
    title VARCHAR(255),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES clients(id),
    INDEX idx_conversations_client (client_id, updated_at)
) ENGINE=InnoDB;

-- 6. Conversation messages and assistant citations
CREATE TABLE IF NOT EXISTS conversation_messages (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    conversation_id BIGINT UNSIGNED NOT NULL,
    role ENUM('system', 'user', 'assistant', 'tool') NOT NULL,
    content TEXT NOT NULL,
    citations JSON NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (conversation_id) REFERENCES conversations(id),
    INDEX idx_messages_conversation (conversation_id, created_at)
) ENGINE=InnoDB;

-- 7. Feedback on copilot responses
CREATE TABLE IF NOT EXISTS feedback (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    conversation_id BIGINT UNSIGNED NOT NULL,
    message_id BIGINT UNSIGNED NULL,
    client_id BIGINT UNSIGNED NOT NULL,
    rating TINYINT UNSIGNED,
    feedback_text TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (conversation_id) REFERENCES conversations(id),
    FOREIGN KEY (message_id) REFERENCES conversation_messages(id),
    FOREIGN KEY (client_id) REFERENCES clients(id),
    CONSTRAINT chk_feedback_rating
        CHECK (rating IS NULL OR rating BETWEEN 1 AND 5),
    INDEX idx_feedback_client (client_id, created_at)
) ENGINE=InnoDB;

-- 8. Escalations: questions requiring human review
CREATE TABLE IF NOT EXISTS escalations (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    client_id BIGINT UNSIGNED NOT NULL,
    conversation_id BIGINT UNSIGNED NULL,
    message_id BIGINT UNSIGNED NULL,
    reason VARCHAR(255) NOT NULL,
    details TEXT,
    status ENUM('open', 'in_review', 'resolved', 'closed')
        NOT NULL DEFAULT 'open',
    assigned_to VARCHAR(200),
    resolution_notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP NULL,
    FOREIGN KEY (client_id) REFERENCES clients(id),
    FOREIGN KEY (conversation_id) REFERENCES conversations(id),
    FOREIGN KEY (message_id) REFERENCES conversation_messages(id),
    INDEX idx_escalations_status (status, created_at),
    INDEX idx_escalations_client (client_id, created_at)
) ENGINE=InnoDB;

-- 9. Security events: audit trail for security-relevant actions
CREATE TABLE IF NOT EXISTS security_events (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    client_id BIGINT UNSIGNED NULL,
    event_type VARCHAR(100) NOT NULL,
    severity ENUM('info', 'warning', 'high', 'critical')
        NOT NULL DEFAULT 'info',
    resource_type VARCHAR(100),
    resource_id VARCHAR(150),
    details JSON NULL,
    request_id VARCHAR(100),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES clients(id),
    INDEX idx_security_event_type (event_type, created_at),
    INDEX idx_security_severity (severity, created_at)
) ENGINE=InnoDB;

-- 10. Ingestion jobs: track PDF processing tasks
CREATE TABLE IF NOT EXISTS ingestion_jobs (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    report_id BIGINT UNSIGNED NOT NULL,
    status ENUM(
        'queued', 'running', 'completed', 'failed'
    ) NOT NULL DEFAULT 'queued',
    attempt_count INT UNSIGNED NOT NULL DEFAULT 0,
    started_at TIMESTAMP NULL,
    finished_at TIMESTAMP NULL,
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (report_id) REFERENCES reports(id),
    INDEX idx_ingestion_status (status, created_at),
    INDEX idx_ingestion_report (report_id)
) ENGINE=InnoDB;

from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

import backend.main as main
from backend.database import ClientStatus, get_db, get_readonly_db
from backend import web_auth

app = main.app


def test_frontend_does_not_embed_api_key_or_persist_browser_credentials() -> None:
    frontend = Path(__file__).resolve().parents[2] / "frontend"
    html = (frontend / "index.html").read_text(encoding="utf-8")
    script = (frontend / "app.js").read_text(encoding="utf-8")
    assert "ATLAS_API_KEY" not in html + script
    assert "X-API-Key" not in script
    assert "localStorage" not in script
    assert "sessionStorage" not in script
    assert "/ui/api/" in script


def test_frontend_is_served_with_security_headers() -> None:
    response = TestClient(app).get("/app/")
    assert response.status_code == 200
    assert "Project Atlas" in response.text
    assert response.headers["content-security-policy"].startswith("default-src 'self'")
    assert response.headers["x-content-type-options"] == "nosniff"


def test_workspace_password_is_documented_as_a_template_placeholder() -> None:
    template = Path(__file__).resolve().parents[2] / ".env.example"
    content = template.read_text(encoding="utf-8")

    assert "ATLAS_WEB_PASSWORD=replace_with_a_separate_long_random_password" in content
    assert (
        "# Placeholder only: set your own strong value in backend/.env; "
        "do not use this example value."
    ) in content


def test_web_session_requires_configured_valid_password(monkeypatch) -> None:
    monkeypatch.setenv("ATLAS_WEB_PASSWORD", "test-password-that-is-never-returned")
    client = TestClient(app)

    invalid = client.post("/ui/api/session", json={"password": "wrong"})
    assert invalid.status_code == 401
    assert "test-password-that-is-never-returned" not in invalid.text

    valid = client.post(
        "/ui/api/session",
        json={"password": "test-password-that-is-never-returned"},
    )
    assert valid.status_code == 200
    cookie = valid.headers["set-cookie"]
    assert "HttpOnly" in cookie
    assert "samesite=strict" in cookie.lower()
    assert "path=/ui/api" in cookie.lower()
    assert "max-age=28800" in cookie.lower()
    assert "secure" not in cookie.lower()
    assert "test-password-that-is-never-returned" not in valid.text
    assert "Cache-Control" in valid.headers
    assert valid.json()["expires_in"] == 28800

    restored = client.get("/ui/api/session")
    assert restored.status_code == 200
    assert restored.json()["csrf_token"] == valid.json()["csrf_token"]
    assert restored.headers["cache-control"] == "no-store"


def test_web_session_cookie_is_secure_for_https(monkeypatch) -> None:
    monkeypatch.setenv("ATLAS_WEB_PASSWORD", "test-password-for-https")
    client = TestClient(app, base_url="https://testserver")
    response = client.post(
        "/ui/api/session",
        json={"password": "test-password-for-https"},
    )

    assert response.status_code == 200
    assert "secure" in response.headers["set-cookie"].lower()
    assert "httponly" in response.headers["set-cookie"].lower()
    assert "samesite=strict" in response.headers["set-cookie"].lower()


def test_web_api_requires_session_and_csrf_token(monkeypatch) -> None:
    monkeypatch.setenv("ATLAS_WEB_PASSWORD", "test-password-for-session")
    client = TestClient(app)

    assert client.get("/ui/api/reports").status_code == 401
    login = client.post("/ui/api/session", json={"password": "test-password-for-session"})
    csrf_token = login.json()["csrf_token"]

    assert client.post(
        "/ui/api/chat",
        json={"client_id": 1, "query": "Summarize a report."},
    ).status_code == 403
    assert client.delete("/ui/api/session").status_code == 403

    logout = client.delete(
        "/ui/api/session",
        headers={"X-CSRF-Token": csrf_token},
    )
    assert logout.status_code == 200
    assert client.get("/ui/api/session").status_code == 401


def test_web_sign_in_is_disabled_without_web_password(monkeypatch) -> None:
    monkeypatch.delenv("ATLAS_WEB_PASSWORD", raising=False)
    response = TestClient(app).post("/ui/api/session", json={"password": "not-configured"})
    assert response.status_code == 503
    assert response.json()["detail"] == (
        "Web sign-in is not configured. Set ATLAS_WEB_PASSWORD in backend/.env "
        "and restart FastAPI."
    )


def test_web_sign_in_rejects_blank_password_configuration(monkeypatch) -> None:
    monkeypatch.setenv("ATLAS_WEB_PASSWORD", "   ")
    response = TestClient(app).post("/ui/api/session", json={"password": "anything"})
    assert response.status_code == 503
    assert "ATLAS_WEB_PASSWORD" in response.json()["detail"]


def test_expired_web_session_is_rejected(monkeypatch) -> None:
    monkeypatch.setenv("ATLAS_WEB_PASSWORD", "test-password-for-expiry")
    client = TestClient(app)
    login = client.post(
        "/ui/api/session",
        json={"password": "test-password-for-expiry"},
    )
    assert login.status_code == 200
    session_id = client.cookies.get(web_auth.SESSION_COOKIE_NAME)
    assert session_id is not None
    web_auth._sessions[session_id] = web_auth.WebSession(
        csrf_token=login.json()["csrf_token"],
        expires_at=0,
    )

    expired = client.get("/ui/api/session")
    assert expired.status_code == 401
    assert expired.json()["detail"] == "Your session has expired. Sign in again."
    assert client.get("/ui/api/reports").status_code == 401


def test_web_chat_reports_unregistered_client_without_starting_workflow(monkeypatch) -> None:
    monkeypatch.setenv("ATLAS_WEB_PASSWORD", "test-password-for-missing-client")
    fake_db = SimpleNamespace(get=lambda *_: None)
    workflow_calls = []

    def unexpected_workflow(*args):
        workflow_calls.append(args)
        raise AssertionError("Workflow must not run for an unregistered client.")

    monkeypatch.setattr(main, "run_report_copilot", unexpected_workflow)
    app.dependency_overrides[get_db] = lambda: fake_db
    app.dependency_overrides[get_readonly_db] = lambda: object()
    client = TestClient(app)
    try:
        login = client.post(
            "/ui/api/session",
            json={"password": "test-password-for-missing-client"},
        )
        non_numeric = client.post(
            "/ui/api/chat",
            json={"client_id": "ABCD", "query": "Summarize an authorized report."},
            headers={"X-CSRF-Token": login.json()["csrf_token"]},
        )
        response = client.post(
            "/ui/api/chat",
            json={"client_id": 987654321, "query": "Summarize an authorized report."},
            headers={"X-CSRF-Token": login.json()["csrf_token"]},
        )

        assert login.status_code == 200
        assert non_numeric.status_code == 422
        assert response.status_code == 404
        assert response.json()["detail"] == (
            "Client ID is not registered. Enter a numeric ID from the clients table."
        )
        assert workflow_calls == []
    finally:
        app.dependency_overrides.pop(get_db, None)
        app.dependency_overrides.pop(get_readonly_db, None)


def test_direct_and_web_chat_reach_the_shared_workflow(monkeypatch) -> None:
    monkeypatch.setenv("ATLAS_API_KEY", "test-only-api-key")
    monkeypatch.setenv("ATLAS_WEB_PASSWORD", "test-only-web-password")
    fake_db = SimpleNamespace(
        get=lambda *_: SimpleNamespace(status=ClientStatus.ACTIVE),
        commit=lambda: None,
        rollback=lambda: None,
    )
    workflow_calls = []

    def fake_workflow(db, readonly_db, client_id, query):
        workflow_calls.append((client_id, query))
        return {
            "answer": "Test answer",
            "citations": [],
            "confidence": 0.9,
            "conflicts": [],
            "derived_values": [],
            "escalated": False,
        }

    monkeypatch.setattr(main, "run_report_copilot", fake_workflow)
    app.dependency_overrides[get_db] = lambda: fake_db
    app.dependency_overrides[get_readonly_db] = lambda: fake_db
    client = TestClient(app)
    try:
        payload = {"client_id": 7, "query": "Summarize the report."}
        unauthenticated = client.post("/chat", json=payload)
        direct = client.post(
            "/chat",
            json=payload,
            headers={"X-API-Key": "test-only-api-key"},
        )
        assert unauthenticated.status_code == 401
        assert direct.status_code == 200
        assert direct.json()["answer"] == "Test answer"

        login = client.post(
            "/ui/api/session",
            json={"password": "test-only-web-password"},
        )
        web = client.post(
            "/ui/api/chat",
            json=payload,
            headers={"X-CSRF-Token": login.json()["csrf_token"]},
        )
        assert login.status_code == 200
        assert web.status_code == 200
        assert web.json()["answer"] == "Test answer"
        assert workflow_calls == [
            (7, "Summarize the report."),
            (7, "Summarize the report."),
        ]
    finally:
        app.dependency_overrides.pop(get_db, None)
        app.dependency_overrides.pop(get_readonly_db, None)


def test_direct_and_browser_upload_routes_use_ingestion_services(monkeypatch) -> None:
    monkeypatch.setenv("ATLAS_API_KEY", "test-only-api-key")
    monkeypatch.setenv("ATLAS_WEB_PASSWORD", "test-only-web-password")
    fake_db = object()
    direct_calls = []
    upload_calls = []

    def fake_ingest_report(db, **kwargs):
        direct_calls.append(kwargs)
        return {"report_id": kwargs["report_id"], "chunks_indexed": 2}

    def fake_ingest_uploaded_report(db, **kwargs):
        upload_calls.append(kwargs)
        return {
            "report_id": kwargs["report_id"],
            "chunks_indexed": 2,
            "source_filename": kwargs["filename"],
        }

    monkeypatch.setattr(main, "ingest_report", fake_ingest_report)
    monkeypatch.setattr(main, "ingest_uploaded_report", fake_ingest_uploaded_report)
    app.dependency_overrides[get_db] = lambda: fake_db
    client = TestClient(app)
    try:
        direct = client.post(
            "/ingest",
            json={
                "report_id": 12,
                "sector": "Energy",
                "region": "Europe",
                "year": 2025,
            },
            headers={"X-API-Key": "test-only-api-key"},
        )
        assert direct.status_code == 200
        assert direct.json()["status"] == "success"
        assert direct_calls[0]["report_id"] == 12

        login = client.post(
            "/ui/api/session",
            json={"password": "test-only-web-password"},
        )
        upload = client.post(
            "/ui/api/ingest",
            headers={"X-CSRF-Token": login.json()["csrf_token"]},
            data={
                "report_id": "13",
                "sector": "Energy",
                "region": "Europe",
                "year": "2025",
            },
            files={"file": ("report.md", b"Verified report content", "text/markdown")},
        )
        assert login.status_code == 200
        assert upload.status_code == 200
        assert upload.json()["status"] == "success"
        assert upload.json()["chunks_indexed"] == 2
        assert upload_calls[0]["report_id"] == 13
        assert upload_calls[0]["content"] == b"Verified report content"
    finally:
        app.dependency_overrides.pop(get_db, None)


def test_upload_request_size_limit_rejects_oversized_body() -> None:
    response = TestClient(app).post(
        "/ui/api/ingest",
        headers={"Content-Length": str(27 * 1024 * 1024)},
    )
    assert response.status_code == 413


def test_upload_endpoint_rejects_unsupported_file_before_database_access(monkeypatch) -> None:
    monkeypatch.setenv("ATLAS_WEB_PASSWORD", "test-upload-password")
    client = TestClient(app)
    login = client.post("/ui/api/session", json={"password": "test-upload-password"})

    response = client.post(
        "/ui/api/ingest",
        headers={"X-CSRF-Token": login.json()["csrf_token"]},
        data={
            "report_id": "1",
            "sector": "Energy",
            "region": "Europe",
            "year": "2025",
        },
        files={"file": ("payload.exe", b"not a report", "application/octet-stream")},
    )
    assert response.status_code == 400
    assert "PDF, Markdown, or HTML" in response.json()["detail"]

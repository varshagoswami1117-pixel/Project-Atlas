from __future__ import annotations

import hmac
import os
import secrets
import threading
import time
from collections import deque
from dataclasses import dataclass

from fastapi import Depends, Header, HTTPException, Request, Response

SESSION_COOKIE_NAME = "atlas_web_session"
SESSION_COOKIE_PATH = "/ui/api"
DEFAULT_SESSION_TTL_SECONDS = 8 * 60 * 60
MAX_LOGIN_ATTEMPTS = 5
LOGIN_WINDOW_SECONDS = 5 * 60
WEB_PASSWORD_SETUP_MESSAGE = (
    "Web sign-in is not configured. Set ATLAS_WEB_PASSWORD in backend/.env "
    "and restart FastAPI."
)


@dataclass(frozen=True)
class WebSession:
    csrf_token: str
    expires_at: float


_sessions: dict[str, WebSession] = {}
_login_attempts: dict[str, deque[float]] = {}
_lock = threading.Lock()


def _session_ttl() -> int:
    configured = os.getenv("ATLAS_WEB_SESSION_TTL_SECONDS")
    if configured is None:
        return DEFAULT_SESSION_TTL_SECONDS
    try:
        ttl = int(configured)
    except ValueError as error:
        raise RuntimeError("ATLAS_WEB_SESSION_TTL_SECONDS must be a positive integer.") from error
    if not 300 <= ttl <= 24 * 60 * 60:
        raise RuntimeError("ATLAS_WEB_SESSION_TTL_SECONDS must be between 300 and 86400.")
    return ttl


def _check_login_rate_limit(client_host: str) -> None:
    now = time.monotonic()
    with _lock:
        attempts = _login_attempts.setdefault(client_host, deque())
        while attempts and now - attempts[0] >= LOGIN_WINDOW_SECONDS:
            attempts.popleft()
        if len(attempts) >= MAX_LOGIN_ATTEMPTS:
            raise HTTPException(status_code=429, detail="Too many sign-in attempts. Try again later.")
        attempts.append(now)


def create_web_session(
    web_password: str,
    request: Request,
    response: Response,
) -> dict[str, int | str]:
    expected_password = os.getenv("ATLAS_WEB_PASSWORD")
    if expected_password is None or not expected_password.strip():
        raise HTTPException(status_code=503, detail=WEB_PASSWORD_SETUP_MESSAGE)

    client_host = request.client.host if request.client is not None else "unknown"
    _check_login_rate_limit(client_host)
    if not hmac.compare_digest(web_password, expected_password):
        raise HTTPException(status_code=401, detail="Invalid workspace password.")

    now = time.monotonic()
    ttl = _session_ttl()
    session_id = secrets.token_urlsafe(32)
    csrf_token = secrets.token_urlsafe(32)
    with _lock:
        expired_ids = [key for key, value in _sessions.items() if value.expires_at <= now]
        for key in expired_ids:
            _sessions.pop(key, None)
        _login_attempts.pop(client_host, None)
        _sessions[session_id] = WebSession(csrf_token=csrf_token, expires_at=now + ttl)

    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=session_id,
        max_age=ttl,
        httponly=True,
        secure=request.url.scheme == "https",
        samesite="strict",
        path=SESSION_COOKIE_PATH,
    )
    response.headers["Cache-Control"] = "no-store"
    return {"csrf_token": csrf_token, "expires_in": ttl}


def get_web_session(request: Request) -> WebSession:
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    if not session_id:
        raise HTTPException(status_code=401, detail="Sign-in required.")
    now = time.monotonic()
    with _lock:
        session = _sessions.get(session_id)
        if session is None or session.expires_at <= now:
            _sessions.pop(session_id, None)
            raise HTTPException(status_code=401, detail="Your session has expired. Sign in again.")
    return session


def require_web_session(request: Request) -> WebSession:
    return get_web_session(request)


def require_web_csrf(
    session: WebSession = Depends(require_web_session),
    csrf_header: str | None = Header(default=None, alias="X-CSRF-Token"),
) -> WebSession:
    if csrf_header is None or not hmac.compare_digest(csrf_header, session.csrf_token):
        raise HTTPException(status_code=403, detail="Invalid or missing CSRF token.")
    return session


def destroy_web_session(request: Request, response: Response) -> None:
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    if session_id:
        with _lock:
            _sessions.pop(session_id, None)
    response.delete_cookie(
        key=SESSION_COOKIE_NAME,
        path=SESSION_COOKIE_PATH,
        httponly=True,
        secure=request.url.scheme == "https",
        samesite="strict",
    )
    response.headers["Cache-Control"] = "no-store"

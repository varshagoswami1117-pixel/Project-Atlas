# Project Atlas — Report Copilot

Project Atlas is a FastAPI application backed by MySQL and a persistent local Chroma vector store. The browser dashboard is a small vanilla HTML, CSS, and JavaScript application served by FastAPI at `/app/`.

## Frontend architecture

The repository did not contain a CodeIgniter or other PHP application when Stage 5 was implemented. The frontend therefore uses static same-origin assets in `frontend/`, served by the existing FastAPI process. This avoids a second runtime, browser-side API URLs, and permissive CORS configuration.

Browser requests use the same-origin `/ui/api/` server-side boundary. Users enter a separate `ATLAS_WEB_PASSWORD` into a sign-in form; FastAPI validates it, clears the browser field after submission, and issues a short-lived HttpOnly, SameSite session cookie. The `ATLAS_API_KEY` stays in the backend environment and is not embedded in frontend files, sent to the browser, or forwarded from JavaScript as an `X-API-Key` header. State-changing browser requests require a CSRF token held in page memory.

The direct API remains available: `POST /chat` and `POST /ingest` continue to require `X-API-Key`. Browser uploads use a separate multipart endpoint, `/ui/api/ingest`, which stores the validated source beneath `data/uploads/` and invokes the same `ingest_report()` pipeline for a report record that already exists in MySQL.

## Local setup

Use the existing Python virtual environment from the workspace root:

```powershell
.\.venv\Scripts\python.exe -m pip install -r .\backend\requirements.txt
```

Set the MySQL values, a strong `ATLAS_API_KEY`, and a separate strong `ATLAS_WEB_PASSWORD` in the private `backend/.env` file. The file is ignored by Git; never paste its contents into source files or support messages. See [.env.example](./.env.example) for variable names and non-secret example values.

If the sign-in response says that web sign-in is not configured, add a strong `ATLAS_WEB_PASSWORD` setting to `backend/.env` and restart FastAPI. The session endpoint deliberately returns HTTP 503 until this setting is present; a `GET /ui/api/session` HTTP 401 before sign-in simply means there is no active session.

To securely generate or reset the workspace password from PowerShell at the workspace root, run the following. It updates only the `ATLAS_WEB_PASSWORD` line, does not print the password, and copies the new value to the clipboard for pasting into the sign-in form:

```powershell
$envPath = (Resolve-Path '.\backend\.env').Path
$random = [System.Security.Cryptography.RandomNumberGenerator]::Create()
$bytes = New-Object byte[] 32
$random.GetBytes($bytes)
$password = [System.BitConverter]::ToString($bytes).Replace('-', '')
$random.Dispose()

$lines = [System.Collections.Generic.List[string]]::new()
$found = $false
foreach ($line in [System.IO.File]::ReadAllLines($envPath)) {
    if ($line -match '^ATLAS_WEB_PASSWORD=') {
        if (-not $found) {
            [void]$lines.Add("ATLAS_WEB_PASSWORD=$password")
            $found = $true
        }
    } else {
        [void]$lines.Add($line)
    }
}
if (-not $found) {
    [void]$lines.Add("ATLAS_WEB_PASSWORD=$password")
}
[System.IO.File]::WriteAllLines(
    $envPath,
    $lines.ToArray(),
    [System.Text.UTF8Encoding]::new($false)
)
Set-Clipboard -Value $password
[Array]::Clear($bytes, 0, $bytes.Length)
$password = $null
Write-Output 'Workspace password updated and copied to the clipboard.'
```

Restart FastAPI after changing the password. A server restart invalidates active browser sessions; sign in again after restarting.

Start FastAPI from the workspace root:

```powershell
.\.venv\Scripts\python.exe -m uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000
```

Open the dashboard at <http://127.0.0.1:8000/app/>. Sign in with the value configured as `ATLAS_WEB_PASSWORD`. The UI checks the public `/health` endpoint and displays API availability. It does not call `/test-db` or expose database diagnostics to browser users.

## Chat workflow

Enter the numeric `client_id` and a question. The request body matches the backend contract:

```json
{
  "client_id": 12,
  "query": "Compare the authorized report findings."
}
```

The workflow uses active, unexpired report purchases in MySQL to filter retrieval. The browser renders returned answers, source citations, derived values, conflicts, confidence, and escalation status as text (not executable HTML). The current agent workflow is source-grounded and deterministic; a generative LLM provider is not configured or claimed by the UI.

Synthetic market metrics are excluded from retrieval by default. For local testing with the explicitly labelled synthetic dataset only, set `ATLAS_INCLUDE_SYNTHETIC_METRICS=true` in `backend/.env` and restart FastAPI. Leave it unset or `false` outside local testing; the opt-in does not bypass client or report entitlements.

## Report ingestion

The upload form lists registered report records from MySQL. Choose one, provide its sector, region, and year, then upload a PDF, Markdown, or HTML file (maximum 25 MiB). The record must exist before upload. The service updates its source path and runs the shared ingestion pipeline. A successful UI message is shown only after FastAPI returns `status: "success"` with the indexed chunk count.

Ingestion does not create a client purchase or grant access. A client can retrieve the indexed report only when an authorized administrator has an active entry in `client_purchases`. The underlying JSON `POST /ingest` contract remains available for pre-positioned workspace documents.

## Synthetic end-to-end dataset

The fully fictional dataset and create-only utility are in `data/synthetic_test_dataset/`. From the workspace root, first inspect the live table counts with the read-only dry run:

```powershell
.\.venv\Scripts\python.exe -m backend.scripts.synthetic_dataset dry-run
```

After reviewing that output, the approved local setup sequence is:

```powershell
.\.venv\Scripts\python.exe -m backend.scripts.synthetic_dataset apply
.\.venv\Scripts\python.exe -m backend.scripts.synthetic_dataset ingest
.\.venv\Scripts\python.exe -m backend.scripts.synthetic_dataset exercise
.\.venv\Scripts\python.exe -m backend.scripts.synthetic_dataset verify
```

The script prints generated numeric client/report IDs; do not substitute guessed IDs. `exercise` uses the configured workspace password internally without printing it, authenticates to the same `/ui/api/` routes, and enables synthetic SQL metrics only for its process. To use synthetic metrics in later local browser chats, set `ATLAS_INCLUDE_SYNTHETIC_METRICS=true` in `backend/.env` and restart FastAPI. Keep this opt-in unset or `false` outside local testing; report entitlement checks remain enforced.

The optional cleanup command is:

```powershell
.\.venv\Scripts\python.exe -m backend.scripts.synthetic_dataset cleanup
```

Cleanup uses the ignored run ledger and synthetic markers, refuses untracked dependent rows or Chroma chunks, and retains the Markdown fixtures and manifest. It does not delete unrelated database rows or reset the schema.

## Verification

Run the backend suite and dependency check from the workspace root:

```powershell
.\.venv\Scripts\python.exe -m pytest .\backend\tests -v
.\.venv\Scripts\python.exe -m pip check
```

Use `http://127.0.0.1:8000/docs` for the API documentation. Do not expose this development server directly to the public internet.

## Roadmap

See [docs/ROADMAP.md](./docs/ROADMAP.md) for implemented, tested, and pending stages.

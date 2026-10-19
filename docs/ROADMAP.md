# Project Atlas Roadmap

Status reflects repository implementation and tests, not a production-readiness certification.

| Stage | Focus | Status | Evidence / remaining work |
|---|---|---|---|
| 1 | FastAPI and MySQL foundation | Implemented; previously verified | Existing FastAPI application and MySQL schema. Re-verify against the target deployment database. |
| 2 | SQLAlchemy models and database lifecycle | Implemented; previously verified | ORM mappings cover the existing relational schema and use pooled sessions. |
| 3 | Document ingestion, metadata chunking, vector store | Implemented; unit/integration components verified | PDF, Markdown, and HTML ingestion with Chroma persistence. Confirm embedding model availability in the target environment. |
| 4 | Agent workflow and entitlement-aware retrieval | Implemented; unit-tested | Deterministic LangGraph router, retriever, calculator, critic, writer, and escalation guard. A generative LLM provider is not wired. |
| 5 | Frontend integration | Implemented; automated and browser-verified | Same-origin FastAPI-served dashboard, server-side web session, CSRF-protected chat and multipart upload. Browser flow and error cases verified. Successful file ingestion still requires a registered MySQL report and available embeddings. |
| 6 | End-to-end integration and workflow verification | Pending | With a seeded MySQL client/report/purchase, ingest a real sample document, ask a grounded question, and verify exact citations and escalation persistence. Test embedding/model setup and restart persistence. |
| 7 | Security, error handling, and performance hardening | Pending | Replace process-local web sessions with a shared production session store; apply a deployment-grade login rate limiter; review role-based staff/client authorization, upload scanning, proxy/body limits, retention, observability, and load behavior. |
| 8 | Deployment preparation and documentation | In progress | Local run instructions and this roadmap are documented. Add deployment configuration, TLS/reverse-proxy guidance, secret provisioning, health/readiness probes, backups, and an operational runbook after deployment choices are made. |

## Architecture decision

No PHP/CodeIgniter project was present in the workspace at Stage 5. The implementation uses static frontend assets served from the existing FastAPI process rather than introducing an unrequested PHP runtime or cross-origin deployment.

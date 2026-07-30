# Healthcare Kafka Pipeline (Python)

## Status
| Phase | Status |
|-------|--------|
| P1 Local Kafka C-CDA | Done |
| P2 Databricks Volume + Delta | Done |
| P3 Chatbot running + docs | Done |
| P4 Multi-format feeds (HL7 + FHIR) | Done |
| **P5 Hardening (schema, PHI, monitor)** | **Done** |

## P5 quick start
```bat
cd /d "C:\Bi g Data\Kafka"
docker compose -f docker-compose.yml -f docker-compose.p5.yml up -d

cd healthcare-pipeline
scripts\setup-p5.bat
scripts\worker.bat
scripts\monitor.bat
```

- Schema Registry: http://localhost:8083  
- Kafka UI: http://localhost:8088  
- Health file: `landing\metrics\health.json`

See `docs\ARCHITECTURE.md` for flow/architecture detail, and `docs\P5_HARDENING.md` for TLS notes and env vars.

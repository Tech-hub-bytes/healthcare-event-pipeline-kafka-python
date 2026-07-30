# Healthcare Event Pipeline (Kafka)

Multi-format clinical event pipeline: **C-CDA**, **HL7 ADT**, and **FHIR** → Kafka → validate/parse → bronze/silver/volume landing → optional Databricks + chatbot.

**Contributor / commits:** [muhammadaziz-ui](https://github.com/muhammadaziz-ui)

## Architecture (short)

```
Sources (C-CDA / HL7 / FHIR)
  → Producers (envelope + JSON Schema)
  → Kafka raw topics
  → Worker (validate → parse → land)
  → clinical.events.normalized + audit
  → optional Databricks volume/Delta → chatbot
```

Failure path: validation/parse errors → `healthcare.dlq` + local `landing/dlq/`.

## Quick start

```bat
cd /d "<this-repo>"
docker compose -f docker-compose.yml -f docker-compose.p5.yml up -d

cd healthcare-pipeline
scripts\setup-p5.bat
scripts\worker.bat
```

| Service | URL |
|---------|-----|
| Kafka | `localhost:9092` |
| Schema Registry | http://localhost:8083 |
| Kafka UI | http://localhost:8088 |

See `healthcare-pipeline/README.md` and `healthcare-pipeline/docs/P5_HARDENING.md`.

## Phases

| Phase | Scope |
|-------|--------|
| P1 | C-CDA → Kafka → local landing |
| P2 | Databricks volume + Delta |
| P3 | Chatbot refresh / deploy |
| P4 | HL7 ADT + FHIR feeds |
| P5 | Schema Registry, PHI controls, monitor, retention |

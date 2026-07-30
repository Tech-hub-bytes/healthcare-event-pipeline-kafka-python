# P5 Hardening Guide

## What P5 adds
| Control | Local implementation |
|---------|----------------------|
| TLS | Optional path documented; keep plaintext for local demo by default |
| Schema Registry | Confluent Schema Registry on host `:8083` (container `:8081`) + local JSON Schema enforcement |
| Monitoring | `monitor.py` → `landing/metrics/health.json` |
| PHI controls | Log redaction, bronze payload strip, retention job, no SSN on envelopes |

## Start P5 services
```bat
cd /d "C:\Bi g Data\Kafka"
docker compose -f docker-compose.yml -f docker-compose.p5.yml up -d
cd healthcare-pipeline
scripts\setup-p5.bat
```

## Environment knobs
```bat
set PIPELINE_SECURITY_MODE=strict
set ENFORCE_JSON_SCHEMA=1
set STRIP_PAYLOAD_FROM_BRONZE=1
set REDACT_PHI_IN_LOGS=1
set SCHEMA_REGISTRY_URL=http://localhost:8083
```

## TLS note
Full broker TLS with the `apache/kafka` image needs keystore/truststore wiring.
For local learning, Schema Registry + JSON Schema + PHI redaction give most of the hardening value.
Use VPN/cloud Kafka with TLS for real PHI environments.

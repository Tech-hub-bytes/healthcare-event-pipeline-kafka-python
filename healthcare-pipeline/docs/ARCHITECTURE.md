# Architecture & Flow

See the root [README.md](../README.md) for the full architecture diagram, topic map, landing zones, and happy/failure flow.

## Message path (detail)

```mermaid
sequenceDiagram
  participant File as Clinical file
  participant Prod as Producer
  participant SR as JSON Schema / Registry
  participant K as Kafka
  participant W as Worker
  participant Land as Landing zones
  participant DBX as Databricks (optional)

  File->>Prod: C-CDA / HL7 / FHIR
  Prod->>Prod: build envelope
  Prod->>SR: validate envelope.schema.json
  alt schema invalid
    Prod-->>Prod: exit / do not publish
  else schema ok
    Prod->>K: raw topic + audit
    K->>W: consume raw
    W->>SR: validate envelope + domain rules
    alt validation / parse fail
      W->>K: healthcare.dlq + audit
      W->>Land: landing/dlq
    else success
      W->>Land: bronze / silver / volume
      W->>K: clinical.events.normalized + audit
      W->>DBX: publish_to_databricks.py (optional)
    end
  end
```

## Feed routing

| Raw topic | Feed | Parser |
|-----------|------|--------|
| `ccda.documents.raw` | ccda | `pipeline/ccda_parser.py` |
| `hl7.adt.raw` | hl7 | `pipeline/hl7_parser.py` |
| `fhir.resources.raw` | fhir | `pipeline/fhir_parser.py` |

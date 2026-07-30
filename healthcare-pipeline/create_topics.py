from __future__ import annotations

from confluent_kafka.admin import AdminClient, NewTopic

from pipeline.config import KAFKA_BROKERS, TOPICS


def main() -> None:
    admin = AdminClient({"bootstrap.servers": KAFKA_BROKERS})
    topic_defs = [
        NewTopic(TOPICS["ccda_raw"], num_partitions=3, replication_factor=1),
        NewTopic(TOPICS["hl7_adt_raw"], num_partitions=3, replication_factor=1),
        NewTopic(TOPICS["fhir_raw"], num_partitions=3, replication_factor=1),
        NewTopic(TOPICS["clinical_normalized"], num_partitions=3, replication_factor=1),
        NewTopic(TOPICS["dlq"], num_partitions=1, replication_factor=1),
        NewTopic(TOPICS["audit"], num_partitions=1, replication_factor=1),
    ]

    fs = admin.create_topics(topic_defs, request_timeout=15)
    print("Topic create results:")
    for topic, future in fs.items():
        try:
            future.result()
            print(f"  + created {topic}")
        except Exception as exc:  # noqa: BLE001
            if "already exists" in str(exc).lower() or "TOPIC_ALREADY_EXISTS" in str(exc):
                print(f"  = exists  {topic}")
            else:
                print(f"  ! {topic}: {exc}")

    md = admin.list_topics(timeout=10)
    print("\nCluster topics:")
    for name in sorted(md.topics.keys()):
        print(f"  - {name}")


if __name__ == "__main__":
    main()

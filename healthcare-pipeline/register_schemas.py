from __future__ import annotations

from pipeline.schema_guard import ensure_registered, schema_registry_up


def main() -> None:
    print("P5 schema registration")
    print(f"  registry up: {schema_registry_up()}")
    for line in ensure_registered():
        print(f"  {line}")


if __name__ == "__main__":
    main()

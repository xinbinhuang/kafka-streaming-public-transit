from pathlib import Path

from avro.schema import RecordSchema
from confluent_kafka import avro


SCHEMA_DIR = Path(__file__).parent / "schemas"


def load_schema(schema: str) -> RecordSchema:
    """Load a given Avro schema"""
    parsed_schema = avro.load(SCHEMA_DIR / schema)
    return parsed_schema


def normalize_station_name(station_name: str) -> str:
    """Normalize a station name"""
    normalized_station_name = (
        station_name.lower()
        .replace("/", "_and_")
        .replace(" ", "_")
        .replace("-", "_")
        .replace("'", "")
    )
    return normalized_station_name

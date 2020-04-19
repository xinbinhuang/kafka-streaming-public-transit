from typing import Dict

NAMESPACE = "org.chicago.cta"


def join_topic_name(*names: str):
    """A helper function to multiple string to create a topic name"""
    return ".".join([name for name in names])


class Connections:
    KAFKA_BROKER: str = "PLAINTEXT://localhost:9092"
    REST_PROXY: str = "http://localhost:8082"
    SCHEMA_REGISTRY: str = "http://localhost:8081"
    CONNECT: str = "http://localhost:8083"
    KSQL: str = "http://localhost:8088"
    POSTGRES: Dict[str, str] = {
        "connection.url": "jdbc:postgresql://postgres:5432/cta",
        "user": "cta_admin",
        "password": "chicago",
    }


class Topics:
    ARRIVALS_PREFIX: str = join_topic_name(NAMESPACE, "station.arrivals")
    TURNSTILES: str = join_topic_name(NAMESPACE, "station.turnstiles")
    TURNSTILES_SUMMARY: str = join_topic_name(TURNSTILES, "summary")
    STATIONS: str = join_topic_name(NAMESPACE, "stations")
    STATIONS_LINE: str = join_topic_name(STATIONS, "line")
    WEATHER: str = join_topic_name(NAMESPACE, "weather")

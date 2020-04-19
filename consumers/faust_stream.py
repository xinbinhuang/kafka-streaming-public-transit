"""Defines trends calculations for stations"""
import logging

import faust

import config


logger = logging.getLogger(__name__)

KAFKA_BROKER = config.Connections.KAFKA_BROKER.replace("PLAINTEXT", "kafka")


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker=KAFKA_BROKER, store="memory://")

topic = app.topic(config.Topics.STATIONS, value_type=Station)
out_topic = app.topic(
    config.Topics.STATIONS_LINE,
    key_type=int,
    value_type=TransformedStation,
    partitions=1,
)

table = app.Table(
    name=config.Topics.STATIONS_LINE,
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)


@app.agent(topic)
async def transform_station(stations):
    async for station in stations:
        if station.red:
            line = "red"
        elif station.green:
            line = "green"
        elif station.blue:
            line = "blue"
        else:
            line = None

        transformed_station = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line,
        )

        table[station.station_id] = transformed_station


if __name__ == "__main__":
    app.main()

"""Creates a turnstile data producer"""
import logging

from config import join_topic_name, Topics
from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware
from models.utils import load_schema, normalize_station_name, RecordSchema


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema: RecordSchema = load_schema("turnstile_key.json")
    value_schema: RecordSchema = load_schema("turnstile_value.json")

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = normalize_station_name(station.name)
        topic_name = join_topic_name(Topics.TURNSTILE_PREFIX, station_name)
        super().__init__(
            topic_name=topic_name,
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=3,
            num_replicas=1,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        logger.debug(
            f"[{timestamp.isoformat()}] Riders count: {num_entries} @ {self.station.name}"
        )
        for _ in range(num_entries):
            try:
                self.producer.produce(
                    topic=self.topic_name,
                    key={"timestamp": self.time_millis()},
                    value={
                        "station_id": self.station.station_id,
                        "station_name": self.station.name,
                        "line": self.station.color.name,
                    },
                )
            except Exception as exc:
                logger.error(f"Failed to send message to Kafka: `{exc}`")

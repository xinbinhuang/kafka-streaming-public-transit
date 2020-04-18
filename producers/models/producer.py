"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

from config import Connections

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set()

    def __init__(
        self,
        topic_name,
        key_schema=None,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self._client = None
        self.broker_properties = {
            "bootstrap.servers": Connections.KAFKA_BROKER,
            "schema.registry.url": Connections.SCHEMA_REGISTRY,
        }

        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema,
        )

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

    @property
    def client(self) -> AdminClient:
        if not self._client:
            self._client = AdminClient(
                {"bootstrap.servers": self.broker_properties["bootstrap.servers"]}
            )
        return self._client

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        if self.topic_exists(self.topic_name):
            logger.info(f"Topic already exists: {self.topic_name}")
            return

        futures = self.client.create_topics(
            [
                NewTopic(
                    self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                )
            ]
        )

        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Topic {topic} created")
            except Exception as exc:
                logger.error(f"Failed to create topic {topic}: {exc}")

    def topic_exists(self, topic: str) -> bool:
        """Check if the topic exists in the Kafka"""
        cluster_metadata = self.client.list_topics(timeout=5.0)
        topics = cluster_metadata.topics
        return topic in topics

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

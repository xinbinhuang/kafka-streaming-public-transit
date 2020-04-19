"""Defines core consumer functionality"""
import logging

from confluent_kafka import OFFSET_BEGINNING
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen

import config

logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest
        self.broker_properties = {
            "bootstrap.servers": config.Connections.KAFKA_BROKER,
            "group.id": config.CONSUMER_GROUP,
        }

        if is_avro is True:
            self.broker_properties["schema.registry.url"] = config.Connections.SCHEMA_REGISTRY
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        if self.offset_earliest:
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING

        logger.info(f"partitions assigned for {self.topic_name_pattern}")
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        try:
            msg = self.consumer.poll(self.consume_timeout)
        except SerializerError as exc:
            logger.error(f"{self.topic_name_pattern} | {exc}")
            return 0

        if not msg:
            logger.debug("No message received.")
            return 0
        elif msg.error():
            logger.error(f"{self.topic_name_pattern} | Consumer error: {msg.error()}")
            return 0
        else:
            self.message_handler(msg)
            return 1

    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()

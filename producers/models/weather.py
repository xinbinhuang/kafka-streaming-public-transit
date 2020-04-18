"""Methods pertaining to weather data"""
from enum import IntEnum
import json
import logging
import random

import requests

from models.producer import Producer
from models.utils import load_schema
from config import Connections, Topics


logger = logging.getLogger(__name__)


class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    rest_proxy_url = Connections.REST_PROXY

    key_schema = load_schema("weather_key.json").to_json()
    value_schema = load_schema("weather_value.json").to_json()

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month):
        super().__init__(
            topic_name=Topics.WEATHER,
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
            num_partitions=2,
            num_replicas=1,
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)

        resp = requests.post(
            f"{Weather.rest_proxy_url}/topics/{self.topic_name}",
            headers={"Content-Type": "application/vnd.kafka.avro.v2+json"},
            data=json.dumps(
                {
                    "key_schema": json.dumps(Weather.key_schema),
                    "value_schema": json.dumps(Weather.value_schema),
                    "records": [
                        {
                            "key": {"timestamp": self.time_millis()},
                            "value": {
                                "temperature": self.temp,
                                "status": self.status.name,
                            },
                        }
                    ],
                }
            ),
        )
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as err:
            logger.error(f"Message delivered via REST Proxy failed: {err}")

        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )

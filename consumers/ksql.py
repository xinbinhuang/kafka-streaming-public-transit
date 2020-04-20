"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import config
import topic_check


logger = logging.getLogger(__name__)

# 1: For the first statement, create a `turnstile` table from your turnstile topic.
#       Make sure to use 'avro' datatype!
# 2: For the second statment, create a `turnstile_summary` table by selecting from the
#       `turnstile` table and grouping on station_id.
#       Make sure to cast the COUNT of station id to `count`
#       Make sure to set the value format to JSON

KSQL_STATEMENT = f"""
CREATE TABLE turnstile (
    station_id INT,
    station_name STRING,
    line STRING
) WITH (
    KAFKA_TOPIC='{config.CtaTopics.TURNSTILES}',
    VALUE_FORMAT='AVRO',
    KEY='station_id'
);

CREATE TABLE turnstile_summary
    WITH (
        KAFKA_TOPIC='{config.CtaTopics.TURNSTILES_SUMMARY}',
        VALUE_FORMAT='JSON'
    ) AS
    SELECT
        station_id,
        COUNT(*) AS count
    FROM turnstile
    GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists(config.CtaTopics.TURNSTILES_SUMMARY) is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{config.Connections.KSQL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()

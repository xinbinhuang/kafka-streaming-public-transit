import pytest

from config import join_topic_name
from models import utils


def test_utils_load_schema():
    schema = utils.load_schema("arrival_key.json")
    assert isinstance(schema, utils.RecordSchema)


@pytest.mark.parametrize(
    "station_name,normalized_station_name",
    [
        ("Upper Case", "upper_case"),
        ("with/slash", "with_and_slash"),
        ("dash's-dot", "dashs_dot"),
    ],
)
def test_utils_normalize_station_name(station_name, normalized_station_name):
    assert utils.normalize_station_name(station_name) == normalized_station_name


@pytest.mark.parametrize(
    "topic_segments,output_topic",
    [
        ("single.segment", "single.segment"),
        (["two", "segment"], "two.segment"),
        (["multiple.topic", "random", "segments"], "multiple.topic.random.segments"),
    ],
)
def test_config_join_topic_name(topic_segments, output_topic):
    if isinstance(topic_segments, str):
        assert join_topic_name(topic_segments) == output_topic
    else:
        assert join_topic_name(*topic_segments) == output_topic

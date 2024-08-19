"""
Test queuing_data_processing. Assert it with observing number of Streaming.processed.
"""
import time
from schemas.schemas import CommandPackage


def test_queuing_data_processing(stream_module, redis_connection, message_data: list[CommandPackage]):
    test_channel = "PytestServer"
    # redis_connection.delete(test_channel)
    for _package in message_data:
        redis_connection.xadd(name=test_channel, fields=_package.model_dump())
    time.sleep(2)
    assert stream_module.processed == len(message_data)

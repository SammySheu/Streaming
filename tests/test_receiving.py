"""
Test when receiving messages and receiving commands. Assert it with observing number of Streaming.processed.
"""
import json
import time
from schemas.schemas import CommandPackage


def test_receiving_message(stream_module, redis_connection, message_data: list[CommandPackage]):
    test_channel = "PytestServer"
    for _package in message_data:
        redis_connection.xadd(name=test_channel, fields=_package.model_dump())
    time.sleep(2)
    assert stream_module.processed == len(message_data)


def test_receiving_command(stream_module, redis_connection, command_data: list[CommandPackage]):
    test_channel = "PytestServer"
    for _package in command_data:
        redis_connection.xadd(
            name=test_channel, fields=_package.model_dump())
    time.sleep(2)
    assert stream_module.processed == len(command_data)


def test_receiving_callback(stream_module, redis_connection, callback_data: list[CommandPackage]):
    test_channel = "PytestServer"
    for _package in callback_data:
        redis_connection.xadd(
            name=test_channel, fields=_package.model_dump())
    time.sleep(2)
    assert stream_module.processed == len(callback_data)


def test_receiving_broadcast(stream_module, redis_connection, broadcast_data: list[CommandPackage]):
    test_channel = "ResolvedChannel"
    for _package in broadcast_data:
        redis_connection.publish(
            channel=test_channel, message=json.dumps(_package.model_dump()))
    time.sleep(2)
    assert stream_module.processed == len(broadcast_data)

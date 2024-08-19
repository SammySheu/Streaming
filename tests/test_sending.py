"""
Test sending messages, commands, and callbacks. Assert it with redis xrange.
"""
from schemas.schemas import CommandPackage
from tests.conftest import get_stream_data


def test_send_message(stream_module, redis_connection, message_data: list[CommandPackage]):
    test_channel = "TestChannel"
    redis_connection.delete(test_channel)
    for _package in message_data:
        stream_module.send_message(
            sending_channel=test_channel, command=_package.command, data=_package.data)
    # Get data from redis stream directly
    stream_data = get_stream_data(
        redis_connection, stream_name=test_channel)
    for _stream_data in stream_data:
        assert _stream_data.values.get("type") == "SHOOT"
    # Make sure number of sending times and length of message data are equal
    assert len(stream_data) == len(message_data)


def test_send_command(stream_module, redis_connection, message_data: list[CommandPackage]):
    test_channel = "TestChannel"
    redis_connection.delete(test_channel)
    for _package in message_data:
        stream_module.send_command(
            sending_channel=test_channel, command=_package.command, data=_package.data)
    # Get data from redis stream directly
    stream_data = get_stream_data(
        redis_connection, stream_name=test_channel)
    for _stream_data in stream_data:
        assert _stream_data.values.get("type") == "CONFIRM"
        assert _stream_data.values.get("token") != ""
    # Make sure number of sending times and length of message data are equal
    assert len(stream_data) == len(message_data)


def test_send_callback(stream_module, redis_connection, message_data: list[CommandPackage]):
    test_channel = "TestChannel"
    redis_connection.delete(test_channel)
    for _package in message_data:
        stream_module.send_callback(
            sending_channel=test_channel, command=_package.command, data=_package.data)
    # Get data from redis stream directly
    stream_data = get_stream_data(
        redis_connection, stream_name=test_channel)
    for _stream_data in stream_data:
        assert _stream_data.values.get("type") == "CALLBACK"
        assert _stream_data.values.get("token") != ""
    # Make sure number of sending times and length of message data are equal
    assert len(stream_data) == len(message_data)

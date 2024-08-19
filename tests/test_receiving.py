"""
Test when receiving messages and receiving commands. Assert it with observing number of Streaming.processed.
"""
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
        redis_connection.xadd(name=test_channel, fields=_package.model_dump())
    time.sleep(2)
    assert stream_module.processed == len(command_data)


def test_sending_command_and_listening(stream_module, command_data: list[CommandPackage]):
    test_channel = "PytestServer"
    for _package in command_data:
        stream_module.send_command(
            sending_channel=test_channel,
            command=_package.command,
            data=_package.data)
    time.sleep(2)
    assert stream_module.resolved == len(command_data)


def test_sending_callback_and_listening(stream_module, callback_data: list[CommandPackage]):
    test_channel = "PytestServer"
    tasks = [stream_module.send_callback(
        sending_channel=test_channel, command=_package.command, data=_package.data) for _package in callback_data]
    responses = stream_module.wait_for_callback(*tasks)
    time.sleep(2)
    assert stream_module.listened == len(callback_data)
    assert stream_module.processed == len(callback_data)
    assert stream_module.resolved == len(callback_data)

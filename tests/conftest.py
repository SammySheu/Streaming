import redis
import json
import pytest
from schemas.schemas import CommandPackage, StreamData
from module import Streaming


@Streaming.cmd_register
def Information(tmp):
    return "Information"


@pytest.fixture()
def redis_connection():
    return redis.Redis(host='localhost', port=6379, db=0)


@pytest.fixture()
def stream_module():
    return Streaming(user_module="Pytest", redis_host="localhost",
                     redis_port=6379, redis_db=0, daemon=True)


@pytest.fixture()
def message_data():
    return [CommandPackage(type="SHOOT", command="Information", data=json.dumps({"msg": f"No.{i} of test point"})) for i in range(100)]


def get_stream_data(redis_connection: redis.Redis, stream_name: str):
    _stream_data = redis_connection.xrange(name=stream_name)
    return [StreamData(item) for item in _stream_data]

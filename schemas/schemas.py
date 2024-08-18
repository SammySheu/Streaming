from pydantic import BaseModel, field_validator, Field, model_validator
from typing import Any
from dataclasses import dataclass
import json


class CommandPackage(BaseModel):
    type: str
    command: str
    data: Any
    channel_name: str = ""
    token: str = ""
    entry_id: str = ""
    response: Any = ""


class StreamInfo(BaseModel):
    length: int
    groups: int
    first_entry: str | None = Field(alias="first-entry")
    last_entry: str | None = Field(alias="last-entry")

    @field_validator("first_entry", "last_entry", mode='before')
    @classmethod
    def convert_first_entry(cls, value):
        return value[0].decode() if value else ""


class GroupInfo(BaseModel):
    consumers: int
    pending: int


class ConsumerInfo(BaseModel):
    pending: int
    idle: int
    inactive: int


@dataclass
class StreamData():
    key: str
    values: Any

    def __init__(self, item: tuple):
        self.key = item[0].decode()
        self.values = self.helper_function(item[1])

    @staticmethod
    def helper_function(data):
        if isinstance(data, bytes):
            return StreamData.helper_function(data.decode())
        elif isinstance(data, list):
            return [StreamData.helper_function(i) for i in data]
        elif isinstance(data, dict):
            return {StreamData.helper_function(k): StreamData.helper_function(v) for k, v in data.items()}
        elif isinstance(data, str):
            try:
                return json.loads(data)
            except json.JSONDecodeError:
                return data


@dataclass
class GroupData():
    stream_name: str
    stream_data: list[StreamData]

    def __init__(self, item: list):
        self.stream_name = item[0].decode()
        self.stream_datas = [StreamData(_pair) for _pair in item[1]]

from pydantic import BaseModel, field_validator, Field, model_validator
from typing import Any
from dataclasses import dataclass, Field as DataField
import json
import orjson


class CommandPackage(BaseModel):
    type: str
    command: str
    data: Any
    channel_name: str = ""
    token: str = ""
    entry_id: str = ""
    response: Any = ""


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


class StreamInfo(BaseModel):
    length: int
    groups: int
    first_entry: StreamData | None = Field(alias="first-entry")
    last_entry: StreamData | None = Field(alias="last-entry")

    @field_validator("first_entry", "last_entry", mode='before')
    @classmethod
    def convert_first_entry(cls, value):
        return StreamData(value) if value else None


class GroupInfo(BaseModel):
    name: str
    consumers: int
    pending: int
    last_delivered_id: str | None = Field(alias="last-delivered-id")
    entries_read: str | None = Field(alias="entries-read")

    @field_validator("name", "last_delivered_id", "entries_read", mode='before')
    @classmethod
    def convert_value(cls, value):
        '''
        Convert bytes to string
        '''
        return value.decode() if value else None


class ConsumerInfo(BaseModel):
    name: str
    pending: int
    idle: int = -1
    inactive: int = -1

    @field_validator("name", mode='before')
    def convert_name(cls, value):
        return value.decode()


class PendingInfo(BaseModel):
    pending: int
    min: str | None
    max: str | None
    consumers: list[ConsumerInfo]

    @field_validator("min", "max", mode='before')
    def convert_min_max(cls, value):
        if value:
            return value.decode()


@dataclass
class GroupData():
    stream_name: str
    stream_data: list[StreamData]

    def __init__(self, item: list):
        self.stream_name = item[0].decode()
        self.stream_datas = [StreamData(_pair) for _pair in item[1]]


@dataclass
class SubscribeData():
    type: str
    pattern: str | None
    channel: str
    data: Any | CommandPackage

    def __post_init__(self):
        if isinstance(self.data, bytes):
            try:
                self.data = CommandPackage(
                    **orjson.loads(self.data))
            except Exception:
                pass


class PendingData(BaseModel):
    message_id: str
    consumer: str
    time_since_delivered: int
    times_delivered: int

    @field_validator("message_id", "consumer", mode='before')
    def convert_value(cls, value):
        return value.decode() if value else None

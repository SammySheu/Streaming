from pydantic import BaseModel, field_validator, Field
from typing import Any


class CommandPackage(BaseModel):
    type: str
    command: str
    data: str
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

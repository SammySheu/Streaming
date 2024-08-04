from pydantic import BaseModel, model_validator, field_validator, Field


class CommandBasic(BaseModel):
    command_type: str
    command: str = ""
    data: str
    token: str = ""


class CommandPackage(BaseModel):
    sending_channel: str = ""
    command_basic: CommandBasic

    @model_validator(mode='before')
    @classmethod
    def tidy_up(cls, model_data: dict):
        _cmd_bsc = CommandBasic(command=model_data.get("command", ""),
                                data=model_data.get("data"),
                                token=model_data.get("token", ""),
                                command_type=model_data.get("command_type"))
        model_data["command_basic"] = _cmd_bsc.model_dump()
        return model_data


class StreamInfo(BaseModel):
    length: int
    groups: int
    first_entry: str | None = Field(alias="first-entry")
    last_entry: str | None = Field(alias="last-entry")

    @field_validator("first_entry", "last_entry", mode='before')
    @classmethod
    def convert_first_entry(cls, value):
        return value[0].decode() if value else ""

    # @field_validator("last-entry", mode='before')
    # @classmethod
    # def convert_last_entry(cls, value):
    #     return value[0].decode() if value else ""


class GroupInfo(BaseModel):
    # name: str
    consumers: int
    pending: int


class ConsumerInfo(BaseModel):
    pending: int
    idle: int
    inactive: int

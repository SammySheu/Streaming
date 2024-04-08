from pydantic import BaseModel

class CommandPackage(BaseModel):
    command: str = ""
    data: str = ""
    token: str = ""

class ConsumerInfo(BaseModel):
    pending: int
    idle: int
    inactive: int
from pydantic import BaseModel

class CommandPackage(BaseModel):
    command: str = ""
    data: str = ""
    token: str = ""
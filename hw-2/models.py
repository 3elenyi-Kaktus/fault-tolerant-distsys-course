from enum import IntEnum
from typing import Optional
from pydantic import BaseModel


class Operation(IntEnum):
    GET = 1
    POST = 2
    PUT = 3
    DELETE = 4


class RaftRequest(BaseModel):
    key: Optional[str] = None
    value: Optional[int] = None


class RaftResponse(BaseModel):
    value: Optional[int | str] = None
from dataclasses import field
from typing import Optional
from pydantic import BaseModel


class CRDTRequest(BaseModel):
    data: list[tuple[str, Optional[int]]] = field(default_factory=list)


class CRDTResponse(BaseModel):
    value: Optional[int | str] = None
from dataclasses import field
from typing import Optional
from pydantic import BaseModel


class CRDTRequest(BaseModel):
    data: dict[str, Optional[int]] = field(default_factory=dict)


class CRDTResponse(BaseModel):
    value: Optional[int | str] = None
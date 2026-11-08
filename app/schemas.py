from datetime import date as Date
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from .config import BREED_TYPE
from .services.utils import age_months


class CowProfile(BaseModel):
    birth_date: Date
    weight_kg: float = Field(gt=0, le=1500)
    type: Literal["beef", "dairy", "dual"]
    breed: Literal["Holstein", "Jersey", "Angus", "Limousin", "Charolais", "Simmental"]
    eleveur_id: Optional[str] = None      # the éleveur who looks after this cow

    @field_validator("birth_date")
    @classmethod
    def valid_birth_date(cls, v: Date):
        if not 0 < age_months(v) <= 240:
            raise ValueError("birth_date must give an age between 1 and 240 months")
        return v

    @model_validator(mode="after")
    def type_matches_breed(self):
        if BREED_TYPE[self.breed] != self.type:
            raise ValueError(f"{self.breed} is a {BREED_TYPE[self.breed]} breed, not {self.type}")
        return self


class AdminAlertIn(BaseModel):
    message: str = Field(min_length=1, max_length=500)
    severity: Literal["info", "warning", "critical"] = "info"
    eleveur_id: Optional[str] = None      # empty = all the admin's éleveurs
    cow_id: Optional[str] = None

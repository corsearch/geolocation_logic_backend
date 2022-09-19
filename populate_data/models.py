from typing import Optional
from typing import List
from sqlmodel import Field, SQLModel, JSON, Column
from sqlalchemy.dialects.postgresql import JSON


class PostalCcode(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    country_code: str
    postal_code: str
    place_name: str
    admin_name1: Optional[int] = None
    admin_code1: Optional[int] = None
    admin_name2: Optional[int] = None
    admin_code2: Optional[int] = None
    admin_name3: Optional[int] = None
    admin_code3: Optional[int] = None
    latitude: float
    longitude: float
    accuracy: str


class Country(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    country_code: str
    name: str
    language: str
    language_code: Optional[int] = None
    country_name: str

class BoundingBox(SQLModel, table=True):
    # https://github.com/tiangolo/sqlmodel/issues/178
    id: Optional[int] = Field(default=None, primary_key=True)
    country_code: str
    postal_code: str
    BoundingBox: List[float] = Field(sa_column=Column(JSON))
    
    # Needed for Column(JSON)
    class Config:
        arbitrary_types_allowed = True



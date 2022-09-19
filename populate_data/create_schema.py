from typing import Optional
from sqlmodel import Field, SQLModel, create_engine

from models import PostalCcode
from models import Country
from  models import BoundingBox

#sqlite_file_name = "geoloc_data.db"
#sqlite_url = f"sqlite:///{sqlite_file_name}"

postgres_url = "postgresql://remi:pwd@localhost:5432/geolocation_data"
engine = create_engine(postgres_url, echo=True)

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)


if __name__ == "__main__":
    create_db_and_tables()

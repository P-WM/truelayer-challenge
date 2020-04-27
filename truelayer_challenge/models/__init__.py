from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, String, DECIMAL
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

MONEY = DECIMAL(scale=4, precision=15)


class Movie(Base):
    __tablename__ = 'movies'
    id = Column(String, primary_key=True)
    title = Column(String)
    budget = Column(MONEY)
    revenue = Column(MONEY)
    ratio = Column(DECIMAL(8, 2))
    production_companies = Column(ARRAY(String))
    url = Column(String)
    abstract = Column(String)


engine = create_engine(
    'postgres+psycopg2://AzureDiamond:hunter2@postgres:5432/true_film')
session = sessionmaker()
session.configure(bind=engine)


def create_all_tables():
    Base.metadata.create_all(engine)

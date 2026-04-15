from sqlalchemy import Column, Text, Date, Integer, Boolean
from sqlalchemy.orm import DeclarativeBase

class Base(DeclarativeBase):
    pass

class DimPatient(Base):
    __tablename__ = "DIM_PATIENT"
    patient_id     = Column(Text, primary_key=True)   # UUID as TEXT in SQLite
    birth_date     = Column(Date)
    death_date     = Column(Date, nullable=True)
    prefix         = Column(Text)
    first_name     = Column(Text)
    last_name      = Column(Text)
    suffix         = Column(Text)
    maiden_name    = Column(Text)
    marital_status = Column(Text)
    race           = Column(Text)
    ethnicity      = Column(Text)
    gender         = Column(Text)
    birth_place    = Column(Text)
    city           = Column(Text)
    county         = Column(Text)
    age            = Column(Integer)
    age_group      = Column(Text)
    is_deceased    = Column(Boolean)

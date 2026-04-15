from sqlalchemy import Column, Text, Integer
from src.models.dim_patient import Base

class DimEncounterClass(Base):
    __tablename__ = "DIM_ENCOUNTER_CLASS"
    encounter_class_id = Column(Integer, primary_key=True, autoincrement=True)
    encounter_class    = Column(Text)

from sqlalchemy import Column, Text, Integer, Float
from src.models.dim_patient import Base

class FactProcedures(Base):
    __tablename__ = "FACT_PROCEDURES"
    procedure_sk       = Column(Integer, primary_key=True, autoincrement=True)
    patient_id         = Column(Text)
    encounter_id       = Column(Text)
    procedure_id       = Column(Integer)
    date_id            = Column(Integer)
    duration_minutes   = Column(Float)
    base_cost          = Column(Float)
    reason_code        = Column(Integer)
    reason_description = Column(Text)

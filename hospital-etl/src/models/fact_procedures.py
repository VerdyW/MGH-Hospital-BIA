from sqlalchemy import Column, Text, Integer, Float
from src.models.dim_patient import Base

class FactProcedures(Base):
    __tablename__ = "FACT_PROCEDURES"
    procedure_sk       = Column(Integer, primary_key=True, autoincrement=True)
    patient_id         = Column(Text)
    encounter_id       = Column(Text)
    procedure_type_id  = Column(Integer)   # FK → DIM_PROCEDURE_TYPE
    clinical_code_id   = Column(Integer)   # FK → DIM_CLINICAL_CODES
    date_id            = Column(Integer)
    start_time_id      = Column(Integer)   # FK → DIM_TIME
    stop_time_id       = Column(Integer)   # FK → DIM_TIME
    duration_minutes   = Column(Float)
    base_cost          = Column(Float)
    reason_code        = Column(Integer)
    reason_description = Column(Text)

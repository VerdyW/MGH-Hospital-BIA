from sqlalchemy import Column, Text, Integer, Float
from src.models.dim_patient import Base

class FactDiagnosis(Base):
    __tablename__ = "FACT_DIAGNOSIS"
    diagnosis_sk       = Column(Integer, primary_key=True, autoincrement=True)
    patient_id         = Column(Text)
    encounter_id       = Column(Text)
    diagnosis_id       = Column(Integer)
    date_id            = Column(Integer)
    is_deceased        = Column(Integer)

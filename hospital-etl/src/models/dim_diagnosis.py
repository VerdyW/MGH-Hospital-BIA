from sqlalchemy import Column, Text, Integer
from src.models.dim_patient import Base

class DimDiagnosis(Base):
    __tablename__ = "DIM_DIAGNOSIS"
    diagnosis_id  = Column(Integer, primary_key=True, autoincrement=True)
    reason_code   = Column(Text)
    reason_description = Column(Text)

from sqlalchemy import Column, Text, Integer
from src.models.dim_patient import Base

class DimClinicalCode(Base):
    __tablename__ = "DIM_CLINICAL_CODES"
    clinical_codes_id  = Column(Integer, primary_key=True, autoincrement=True)
    clinical_code   = Column(Text)
    clinical_description = Column(Text)

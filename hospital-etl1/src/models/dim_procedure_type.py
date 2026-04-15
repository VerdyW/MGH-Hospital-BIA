from sqlalchemy import Column, Text, Integer
from src.models.dim_patient import Base

class DimProcedureType(Base):
    __tablename__ = "DIM_PROCEDURE_TYPE"
    procedure_type_id  = Column(Integer, primary_key=True, autoincrement=True)
    name        = Column(Text)

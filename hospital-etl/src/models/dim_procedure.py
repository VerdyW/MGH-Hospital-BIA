from sqlalchemy import Column, Text, Integer, Boolean
from src.models.dim_patient import Base

class DimProcedure(Base):
    __tablename__ = "DIM_PROCEDURE"
    procedure_id  = Column(Integer, primary_key=True, autoincrement=True)
    code          = Column(Text)
    description   = Column(Text)
    is_admission  = Column(Boolean)
    is_therapy    = Column(Boolean)
    is_procedure  = Column(Boolean)

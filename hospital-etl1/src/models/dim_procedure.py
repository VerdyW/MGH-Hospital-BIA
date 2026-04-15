from sqlalchemy import Column, Text, Integer
from src.models.dim_patient import Base

class DimProcedure(Base):
    __tablename__ = "DIM_PROCEDURE"
    procedure_id       = Column(Integer, primary_key=True, autoincrement=True)
    code               = Column(Text)
    description        = Column(Text)
    procedure_category = Column(Text)

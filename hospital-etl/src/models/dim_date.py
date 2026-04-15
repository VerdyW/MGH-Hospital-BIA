from sqlalchemy import Column, Text, Integer
from src.models.dim_patient import Base

class DimDate(Base):
    __tablename__ = "DIM_DATE"
    date_id    = Column(Integer, primary_key=True)  # YYYYMMDD int key
    full_date  = Column(Text)
    year       = Column(Integer)
    quarter    = Column(Integer)
    month      = Column(Integer)
    month_name = Column(Text)
    week       = Column(Integer)
    day        = Column(Integer)
    day_name   = Column(Text)

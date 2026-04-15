from sqlalchemy import Column, Text, Integer, Boolean
from src.models.dim_patient import Base

class DimTime(Base):
    __tablename__ = "DIM_TIME"
    time_key           = Column(Integer, primary_key=True)  # HHMM e.g. 0930
    full_time          = Column(Text)                       # "09:30"
    hour24             = Column(Integer)
    hour12             = Column(Integer)
    am_pm              = Column(Text)
    minute             = Column(Integer)
    interval_30min     = Column(Text)                       # "09:00-09:30"
    interval_1hour     = Column(Text)                       # "09:00-10:00"
    time_of_day        = Column(Text)
    is_business_hours  = Column(Boolean)

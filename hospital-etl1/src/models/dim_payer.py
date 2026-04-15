from sqlalchemy import Column, Text
from src.models.dim_patient import Base

class DimPayer(Base):
    __tablename__ = "DIM_PAYER"
    payer_id = Column(Text, primary_key=True)
    name     = Column(Text)
    address  = Column(Text)
    city     = Column(Text)
    state    = Column(Text)
    zip      = Column(Text)
    phone    = Column(Text)

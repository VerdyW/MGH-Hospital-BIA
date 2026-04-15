from sqlalchemy import Column, Text
from src.models.dim_patient import Base

class DimOrganization(Base):
    __tablename__ = "DIM_ORGANIZATION"
    organization_id = Column(Text, primary_key=True)
    name            = Column(Text)
    address         = Column(Text)
    city            = Column(Text)
    state           = Column(Text)
    zip             = Column(Text)

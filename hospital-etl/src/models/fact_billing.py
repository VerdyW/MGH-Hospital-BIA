from sqlalchemy import Column, Text, Integer, Float
from src.models.dim_patient import Base

class FactBilling(Base):
    __tablename__ = "FACT_BILLING"
    billing_sk          = Column(Integer, primary_key=True, autoincrement=True)
    encounter_id        = Column(Text)
    patient_id          = Column(Text)
    payer_id            = Column(Text)
    date_id             = Column(Integer)
    base_encounter_cost = Column(Float)
    total_claim_cost    = Column(Float)
    payer_coverage      = Column(Float)
    patient_cost        = Column(Float)   # derived: total - payer_coverage

from sqlalchemy import Column, Text, Integer, Float
from src.models.dim_patient import Base

class FactEncounter(Base):
    __tablename__ = "FACT_ENCOUNTER"
    encounter_id          = Column(Text, primary_key=True)
    patient_id            = Column(Text)
    organization_id       = Column(Text)
    payer_id              = Column(Text)
    encounter_class_id    = Column(Integer)
    date_id               = Column(Integer)
    code                  = Column(Text)
    description           = Column(Text)
    los_minutes           = Column(Float)
    base_encounter_cost   = Column(Float)
    total_claim_cost      = Column(Float)
    payer_coverage        = Column(Float)
    reason_code           = Column(Integer)
    reason_description    = Column(Text)

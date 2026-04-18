# Procedure category rules (3-tier classification)
PROCEDURE_ADMISSION_KEYWORDS = [
    "admission", "transfer", "triage", "intake", "registration", "admit", 
]
PROCEDURE_THERAPY_KEYWORDS = [
    "therapy", "regime", "treatment", "dialysis", "chemotherapy",
    "medication", "injection", "infusion", "immunotherapy", "rehabilitation"
]

# Age group bins
AGE_BINS   = [0, 18, 35, 50, 65, 999]
AGE_LABELS = ["0-17", "18-34", "35-49", "50-64", "65+"]

# SQLite table names
TABLES = {
    "dim_patient":        "DIM_PATIENT",
    "dim_date":           "DIM_DATE",
    "dim_payer":          "DIM_PAYER",
    "dim_organization":   "DIM_ORGANIZATION",
    "dim_encounter_class":"DIM_ENCOUNTER_CLASS",
    "dim_procedure":      "DIM_PROCEDURE",
    "dim_diagnosis":      "DIM_DIAGNOSIS",
    "fact_encounter":     "FACT_ENCOUNTER",
    "fact_procedures":    "FACT_PROCEDURES",
    "fact_diagnosis":     "FACT_DIAGNOSIS",
    "fact_billing":       "FACT_BILLING",
}

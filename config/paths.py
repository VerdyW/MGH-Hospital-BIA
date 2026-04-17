from pathlib import Path

BASE_DIR   = Path(__file__).resolve().parent.parent
RAW_DIR    = BASE_DIR / "data" / "raw"
STAGING_DIR = BASE_DIR / "data" / "staging"
OUTPUT_DIR = BASE_DIR / "data" / "output"
DB_PATH    = OUTPUT_DIR / "hospital_dw.db"

# Raw source files
RAW_PATIENTS      = RAW_DIR / "patients.csv"
RAW_ENCOUNTERS    = RAW_DIR / "encounters.csv"
RAW_PROCEDURES    = RAW_DIR / "procedures.csv"
RAW_PAYERS        = RAW_DIR / "payers.csv"
RAW_ORGANIZATIONS = RAW_DIR / "organizations.csv"

# Staging (cleaned CSVs)
STAGING_PATIENTS      = STAGING_DIR / "cleaned_patients.csv"
STAGING_ENCOUNTERS    = STAGING_DIR / "cleaned_encounters.csv"
STAGING_PROCEDURES    = STAGING_DIR / "cleaned_procedures.csv"
STAGING_PAYERS        = STAGING_DIR / "cleaned_payers.csv"
STAGING_ORGANIZATIONS = STAGING_DIR / "cleaned_organizations.csv"

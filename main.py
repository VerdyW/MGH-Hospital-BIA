import sys
import os
from pathlib import Path
from config.paths import DB_PATH
sys.path.insert(0, os.path.dirname(__file__))

from src.utils.logger import logger
from src.utils.validators import validate
from config.paths import (
    RAW_PATIENTS, RAW_ENCOUNTERS, RAW_PROCEDURES,
    RAW_PAYERS, RAW_ORGANIZATIONS,
    STAGING_PATIENTS, STAGING_ENCOUNTERS, STAGING_PROCEDURES,
    STAGING_PAYERS, STAGING_ORGANIZATIONS,
    STAGING_DIR,
)
from src.extract.csv_reader import read_csv

from src.transform.clean_patients      import clean_patients
from src.transform.clean_encounters    import clean_encounters
from src.transform.clean_procedures    import clean_procedures
from src.transform.clean_payers        import clean_payers
from src.transform.clean_organizations import clean_organizations

from src.load.load_dimensions import (
    build_dim_date,
    build_dim_time,
    build_dim_encounter_class,
    build_dim_procedure_type,
    build_dim_clinical_code,
)
from src.load.load_facts import (
    build_fact_encounter,
    build_fact_procedures,
    build_fact_diagnosis,
    build_fact_billing,
)
from src.load.db_loader import get_engine, load_table

# helper replace function
def confirm_db_replace() -> bool:
    """Prompt user to confirm replacing existing database file."""
    if not Path(DB_PATH).exists():
        return True

    logger.warning(f"Database already exists at: {DB_PATH}")
    logger.warning("All existing tables will be permanently deleted.")

    print("Replace existing database? [y/n]: ", end="", flush=True)
    answer = input().strip().lower()

    if answer == "y":
        os.remove(DB_PATH)
        logger.info("Database deleted. Rebuilding from scratch...")
        return True
    else:
        logger.info("Pipeline cancelled. Existing database untouched.")
        return False

def run():
    logger.info("=" * 55)
    logger.info("  HOSPITAL DATA WAREHOUSE — ETL PIPELINE")
    logger.info("=" * 55)

    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    # 1. = EXTRACT ========================
    logger.info("\n[1/3] EXTRACT")
    raw_patients      = read_csv(RAW_PATIENTS,      "patients")
    raw_encounters    = read_csv(RAW_ENCOUNTERS,    "encounters")
    raw_procedures    = read_csv(RAW_PROCEDURES,    "procedures")
    raw_payers        = read_csv(RAW_PAYERS,        "payers")
    raw_organizations = read_csv(RAW_ORGANIZATIONS, "organizations")

    # 2. = TRANSFORM =======================
    logger.info("\n[2/3] TRANSFORM")
    patients      = clean_patients(raw_patients)
    encounters    = clean_encounters(raw_encounters)
    procedures    = clean_procedures(raw_procedures, encounters)
    payers        = clean_payers(raw_payers)
    organizations = clean_organizations(raw_organizations)

    validate(patients,      "DIM_PATIENT")
    validate(encounters,    "FACT_ENCOUNTER (staged)")
    validate(procedures,    "FACT_PROCEDURES (staged)")
    validate(payers,        "DIM_PAYER")
    validate(organizations, "DIM_ORGANIZATION")

    patients.to_csv(STAGING_PATIENTS,           index=False)
    encounters.to_csv(STAGING_ENCOUNTERS,       index=False)
    procedures.to_csv(STAGING_PROCEDURES,       index=False)
    payers.to_csv(STAGING_PAYERS,               index=False)
    organizations.to_csv(STAGING_ORGANIZATIONS, index=False)
    logger.info("[STAGING] All cleaned CSVs saved to data/staging/")
    if not confirm_db_replace():
        return
    # = BUILD DIMENSIONS ===================
    logger.info("\n[3/3] LOAD")
    dim_date            = build_dim_date(encounters)
    dim_time            = build_dim_time()
    dim_encounter_class = build_dim_encounter_class(encounters)
    dim_procedure_type  = build_dim_procedure_type(procedures)
    dim_clinical_code   = build_dim_clinical_code(encounters, procedures)

    # = BUILD FACTS ======================
    fact_encounter  = build_fact_encounter(
        encounters, dim_encounter_class, dim_clinical_code, dim_date
    )
    fact_procedures = build_fact_procedures(
        procedures, dim_procedure_type, dim_clinical_code
    )
    fact_diagnosis  = build_fact_diagnosis(encounters, dim_clinical_code, dim_encounter_class)

    # Enrich fact_diagnosis with is_deceased from patients
    fact_diagnosis = (
        fact_diagnosis
        .merge(patients[["patient_id", "is_deceased"]], on="patient_id", how="left",
               suffixes=("_old", ""))
        .drop(columns=["is_deceased_old"], errors="ignore")
        .rename(columns={"is_deceased": "mortality_flag"})
    )

    fact_billing = build_fact_billing(encounters, dim_encounter_class, dim_clinical_code)

    # 3. = LOAD TO SQLITE ====================
    engine = get_engine()

    load_table(patients,            "DIM_PATIENT",         engine)
    load_table(dim_date,            "DIM_DATE",            engine)
    load_table(dim_time,            "DIM_TIME",            engine)
    load_table(payers,              "DIM_PAYER",           engine)
    load_table(organizations,       "DIM_ORGANIZATION",    engine)
    load_table(dim_encounter_class, "DIM_ENCOUNTER_CLASS", engine)
    load_table(dim_procedure_type,  "DIM_PROCEDURE_TYPE",  engine)
    load_table(dim_clinical_code,   "DIM_CLINICAL_CODES",  engine)

    load_table(fact_encounter,  "FACT_ENCOUNTER",  engine)
    load_table(fact_procedures, "FACT_PROCEDURES", engine)
    load_table(fact_diagnosis,  "FACT_DIAGNOSIS",  engine)
    load_table(fact_billing,    "FACT_BILLING",    engine)

    logger.info("\n" + "=" * 55)
    logger.info("  PIPELINE COMPLETE")
    logger.info(f"  DB -> {engine.url}")
    logger.info("=" * 55)


if __name__ == "__main__":
    run()

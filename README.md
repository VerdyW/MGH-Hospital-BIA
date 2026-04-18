# Healthcare Analytics Data Warehouse
### COMP8047041 — Business Intelligence & Analytics

A data warehouse and ETL pipeline built on synthetic healthcare data, designed to support multi-dimensional analytics across clinical operations, billing, and provider management.

---

## Overview

This project implements a full ETL pipeline that ingests raw healthcare CSV data, transforms it into a clean, analytics-ready format, and loads it into a star schema data warehouse. The warehouse is structured around five core business processes: Patient Registration & Admissions, Encounter Management, Clinical Procedures, Diagnosis & Treatment, and Billing & Insurance Claims.

---

## Data Sources

Six CSV source files extracted from a synthetic healthcare dataset (Synthea):

| File | Description | Rows |
|---|---|---|
| `patients.csv` | Patient demographics and personal attributes | 974 |
| `encounters.csv` | Clinical visit records | 27,891 |
| `procedures.csv` | Medical procedures performed per encounter | 47,701 |
| `payers.csv` | Insurance payer information | 10 |
| `organizations.csv` | Healthcare organization details | 1 |
| `data_dictionary.csv` | Field-level descriptions for all tables | 65 |

---

## Data Warehouse Schema

The warehouse follows a **star schema** design with four fact tables and eight dimension tables.

### Fact Tables

| Table | Description |
|---|---|
| `FACT_ENCOUNTER` | Encounter activity, duration, and cost metrics |
| `FACT_PROCEDURES` | Procedure execution, duration, and cost per encounter |
| `FACT_DIAGNOSIS` | Diagnosis records linked to encounters |
| `FACT_BILLING` | Claim costs, payer coverage, and out-of-pocket breakdown |

### Dimension Tables

| Table | Description |
|---|---|
| `DIM_PATIENT` | Patient demographics, age group, and mortality status |
| `DIM_DATE` | Calendar date attributes (year, month, quarter, week) |
| `DIM_TIME` | Time-of-day attributes (hour, minute, period) |
| `DIM_PAYER` | Insurance payer name and location |
| `DIM_ORGANIZATION` | Healthcare organization details |
| `DIM_ENCOUNTER_CLASS` | Encounter class labels (inpatient, emergency, etc.) |
| `DIM_PROCEDURE_TYPE` | Procedure category (Admission, Therapy/Regime, Procedure) |
| `DIM_CLINICAL_CODES` | Shared clinical codes and descriptions across encounters and procedures |

---

## ETL Pipeline

The pipeline is implemented in Python and runs as a single orchestrated script.

### Extract

Raw CSV files are read directly from the local filesystem using `pandas.read_csv`. No external database connection or API is required at the extraction stage.

### Transform

Each source table undergoes a dedicated cleaning function before being integrated into the warehouse schema. The key transformations applied per table are summarized below.

**Patients**
- Column renaming from `ALL_CAPS` to `snake_case`
- Date parsing for `birth_date` and `death_date`
- Decomposition of `birth_place` into `birth_city`, `birth_state`, and `birth_country`
- Derivation of `age`, `age_group`, and `is_deceased` flag
- Standardization of categorical values (`gender`, `marital_status`, `race`, `ethnicity`)

**Encounters**
- Column renaming and datetime parsing for `start_time` and `stop_time`
- Derivation of `duration_days` and `duration_hours`
- Normalization of `encounter_class` to lowercase
- Null-filling of `reason_code` and `reason_description`

**Procedures**
- Column renaming and datetime parsing
- Derivation of `duration_minutes` and `duration_hours`
- Rule-based classification of procedures into `procedure_category` prior to description cleaning
- Regex-based removal of parenthetical tags (e.g., `(procedure)`, `(regime/therapy)`) from `description`
- Enrichment of `organization_id` via lookup from the encounters table

**Payers**
- Column renaming
- Conversion of `zip` from float to zero-padded string
- Null-filling of address-related fields with `"Unknown"`

**Organizations**
- Column renaming
- Title-case formatting for `name`; uppercase formatting for `state`
- Zero-padding of `zip` to five digits
- Exclusion of `lat` and `lon` columns from the output

In addition to source table cleaning, the following dimension tables are **generated programmatically** during the transform stage:

- `DIM_DATE` — generated from the full date range observed across encounters and procedures
- `DIM_TIME` — generated as a complete 24-hour time table at minute granularity
- `DIM_ENCOUNTER_CLASS` — extracted and deduplicated from the encounters table
- `DIM_PROCEDURE_TYPE` — extracted from the procedure category classification results
- `DIM_CLINICAL_CODES` — extracted and deduplicated from clinical codes across encounters and procedures

### Load

Transformed DataFrames are loaded into the target database using a `load_table` utility function that writes each DataFrame via a SQLAlchemy engine connection. Dimension tables are loaded before fact tables to preserve referential integrity.

---

## Project Structure

```
.
├── config/*.py
├── data/
│   ├── output/.db & .log
│   ├── raw/*.csv
│   ├── staging/*.csv
├── schemas/
│   ├── Fact_Encounters_Star_Schema.png
│   ├── Fact_Procedures_Star_Schema.png
│   ├── Fact_Diagnosis_Star_Schema.png
│   ├── Fact_Billing_Star_Schema.png
│   ├── Actual_Data_ERD.png
│   └── Designed_3NF_ERD.png
├── src/
│   ├── extract/..
│   ├── transform/..
│   ├── load/..
│   └── utils/..
├── main.py
└── README.md
```

---

## Tech Stack

| Component | Tool |
|---|---|
| Language | Python 3.12 |
| Data manipulation | pandas |
| Database connection | SQLAlchemy |
| Schema design | draw.io |
| Target database | PostgreSQL / SQLite |

---

## Business Processes Covered

1. **Patient Registration & Admissions** — patient demographics, age distribution, mortality
2. **Encounter Management** — visit volume, duration, encounter class distribution
3. **Clinical Procedures** — procedure categorization, frequency, and cost
4. **Diagnosis & Treatment** — diagnosis codes, condition prevalence, mortality by condition
5. **Billing & Insurance Claims** — claim costs, payer coverage, out-of-pocket breakdown

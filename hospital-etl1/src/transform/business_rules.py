from config.settings import PROCEDURE_ADMISSION_KEYWORDS, PROCEDURE_THERAPY_KEYWORDS


def classify_procedure(description: str) -> str:
    """
    3-tier procedure categorisation:
      1. Admission   — intake / triage / transfer
      2. Therapy     — ongoing treatment / regime / injection
      3. Procedure   — catch-all (including entries without SNOMED tags e.g. Colonoscopy)
    """
    if not isinstance(description, str):
        return "Procedure"

    desc_lower = description.lower()

    if any(kw in desc_lower for kw in PROCEDURE_ADMISSION_KEYWORDS):
        return "Admission"
    if any(kw in desc_lower for kw in PROCEDURE_THERAPY_KEYWORDS):
        return "Therapy/Regime"
    return "Procedure"

import pandas as pd
from src.utils.logger import logger


def validate(df: pd.DataFrame, name: str, expected_cols: list = None):
    """Simple validation: row count, nulls, and optional column check."""
    logger.info(f"  [{name}] {len(df)} rows | {len(df.columns)} cols")

    null_counts = df.isnull().sum()
    unexpected = null_counts[null_counts > 0]
    # death_date nulls are expected — skip
    unexpected = unexpected.drop(labels=["death_date"], errors="ignore")
    if not unexpected.empty:
        logger.warning(f"  [{name}] Nulls found: {unexpected.to_dict()}")
    else:
        logger.info(f"  [{name}] No unexpected nulls")

    if expected_cols:
        missing = [c for c in expected_cols if c not in df.columns]
        if missing:
            logger.warning(f"  [{name}] Missing columns: {missing}")

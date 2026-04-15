import pandas as pd
from src.utils.logger import logger


def read_csv(path, name: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    logger.info(f"[EXTRACT] {name}: {len(df)} rows loaded from {path.name}")
    return df

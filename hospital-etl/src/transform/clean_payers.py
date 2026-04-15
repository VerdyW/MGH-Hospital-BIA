import pandas as pd
from src.utils.helpers import zip_to_str
from src.utils.logger import logger


def clean_payers(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[TRANSFORM] Cleaning payers...")

    df = df.rename(columns={
        "Id": "payer_id", "NAME": "name", "ADDRESS": "address",
        "CITY": "city", "STATE_HEADQUARTERED": "state_headquartered", "ZIP": "zip", "PHONE": "phone",
    })

    df["zip"]                   = zip_to_str(df["zip"])
    df["address"]               = df["address"].fillna("Unknown")
    df["city"]                  = df["city"].fillna("Unknown")
    df["state_headquartered"]   = df["state_headquartered"].fillna("Unknown")
    df["phone"]                 = df["phone"].fillna("Unknown")

    return df[["payer_id", "name", "address", "city", "state_headquartered", "zip", "phone"]]

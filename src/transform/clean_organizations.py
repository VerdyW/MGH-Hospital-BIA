import pandas as pd
from src.utils.logger import logger
from src.utils.helpers import zip_to_str


def clean_organizations(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[TRANSFORM] Cleaning organizations...")

    df = df.rename(columns={
        "Id": "organization_id", "NAME": "name", "ADDRESS": "address",
        "CITY": "city", "STATE": "state", "ZIP": "zip",
        "LAT": "lat", "LON": "lon",
    })

    df["name"]  = df["name"].str.title()
    df["address"] = df["address"].fillna("Unknown")
    df["zip"]   = zip_to_str(df["zip"])
    df["state"] = df["state"].str.upper()

    return df[["organization_id", "name", "address", "city", "state", "zip"]]

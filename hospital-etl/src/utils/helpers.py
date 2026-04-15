import pandas as pd


def parse_datetime(series: pd.Series) -> pd.Series:
    """Parse ISO datetime strings to pandas datetime."""
    return pd.to_datetime(series, utc=True, errors="coerce").dt.tz_localize(None)


def duration_minutes(start: pd.Series, stop: pd.Series) -> pd.Series:
    """Return duration in minutes between two datetime series."""
    return ((stop - start).dt.total_seconds() / 60).round(2)


def zip_to_str(series: pd.Series) -> pd.Series:
    """Convert float ZIP codes to zero-padded 5-digit strings."""
    return series.apply(lambda z: str(int(z)).zfill(5) if pd.notna(z) else "Unknown")

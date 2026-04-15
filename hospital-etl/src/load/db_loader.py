from sqlalchemy import create_engine, text
from src.utils.logger import logger
from config.paths import DB_PATH


def get_engine():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)
    return engine


def load_table(df, table_name: str, engine, if_exists: str = "replace"):
    """Load a DataFrame into a SQLite table."""
    df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
    logger.info(f"[LOAD] {table_name}: {len(df)} rows loaded")

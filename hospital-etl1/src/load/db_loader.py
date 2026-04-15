from sqlalchemy import create_engine, text
from src.utils.logger import logger
from config.paths import DB_PATH


def get_engine():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)
    return engine


def create_all_tables(engine):
    """Create all tables from models if they don't exist."""
    from src.models.dim_patient import Base as B1
    from src.models.dim_date import Base
    from src.models.fact_encounter import Base
    from src.models.fact_procedures import Base
    from src.models.fact_diagnosis import Base
    from src.models.fact_billing import Base

    B1.metadata.create_all(engine)
    logger.info("[DB] All tables created (if not exist)")


def load_table(df, table_name: str, engine, if_exists: str = "replace"):
    """Load a DataFrame into a SQLite table."""
    df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
    logger.info(f"[LOAD] {table_name}: {len(df)} rows loaded")

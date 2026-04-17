from sqlalchemy import create_engine, text
from src.utils.logger import logger
from config.paths import DB_PATH


def get_engine():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)
    return engine


def load_table(df, table_name: str, engine):
    """Load a DataFrame into a SQLite table, prompting user before replacing."""
    with engine.connect() as conn:
        exists = engine.dialect.has_table(conn, table_name)

    if exists:
        answer = input(f"  Table '{table_name}' already exists. Replace? [y/n]: ").strip().lower()
        if answer != "y":
            logger.info(f"[LOAD] {table_name}: skipped by user")
            return
        with engine.connect() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
            conn.commit()
        logger.info(f"[LOAD] {table_name}: dropped")

    df.to_sql(table_name, con=engine, if_exists="fail", index=False)
    logger.info(f"[LOAD] {table_name}: {len(df)} rows loaded")

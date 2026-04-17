import logging
import sys
from pathlib import Path
from config.paths import OUTPUT_DIR

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(OUTPUT_DIR / "etl.log", mode="w"),
    ]
)

logger = logging.getLogger("hospital_etl")

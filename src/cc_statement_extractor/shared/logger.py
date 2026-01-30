import logging
import sys
from cc_statement_extractor.shared.config import Config

logger = logging.getLogger(__name__)

config = Config()

logger.setLevel(config.get("logging.level", "INFO"))

formatter = logging.Formatter(
    "[%(levelname)s] [%(module)s] %(asctime)s - %(message)s"
)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)

logger.addHandler(handler)
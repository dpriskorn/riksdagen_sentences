import logging

import config
from models.analyzer import Analyzer

logging.basicConfig(level=config.loglevel)


if __name__ == "__main__":
    analyzer = Analyzer()
    analyzer.handle_arguments()

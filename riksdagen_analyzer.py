import logging

import config
from models.riksdagen_analyzer import RiksdagenAnalyzer

logging.basicConfig(level=config.loglevel)


if __name__ == "__main__":
    analyzer = RiksdagenAnalyzer()
    analyzer.handle_arguments()

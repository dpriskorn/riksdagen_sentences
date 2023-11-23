import logging

import config
from models.riksdagen_analyzer import RiksdagenAnalyzer

logging.basicConfig(level=config.loglevel)

ra = RiksdagenAnalyzer(riksdagen_document_type="proposition")
ra.start_analyzing()
# print(ra.df)

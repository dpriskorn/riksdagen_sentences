import logging

import config
from models.riksdagen_analyzer import RiksdagenAnalyzer

logging.basicConfig(level=config.loglevel)


ra = RiksdagenAnalyzer(riksdagen_document_type="departementserien")
ra.start()
# print(ra.df)

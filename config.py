import logging
from datetime import datetime

supported_riksdagen_document_types = {
    "departementserien": {
        "filename": "departementserien",
        "workdirectory": "data/sv/departementserien",
        "wikidata_qid": "Q123501464",
    },
    "proposition": {
        "filename": "proposition",
        "workdirectory": "data/sv/proposition",
        "wikidata_qid": "Q123501430",
    },
}
fasttext_model = "lid.176.bin"
fasttext_model_download_date = datetime.strptime("2023-12-21", "%Y-%m-%d").date()
loglevel = logging.INFO

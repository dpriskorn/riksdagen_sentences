import logging

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
loglevel = logging.WARNING
max_documents_to_extract = 20000 # proposition contains ~11000 documents in total
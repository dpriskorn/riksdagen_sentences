import logging

from pydantic import BaseModel

import config
from models.database_handler import DatabaseHandler

logger = logging.getLogger(__name__)


class DatasetHandler(BaseModel):
    riksdagen_dataset_title: str = ""
    dataset_id: int = 0
    collection_id: int = 1  # hardcoded for now
    database_handler: DatabaseHandler = None

    @property
    def workdirectory(self) -> str:
        return config.supported_riksdagen_document_types[self.riksdagen_dataset_title][
            "workdirectory"
        ]

    @property
    def filename(self) -> str:
        return config.supported_riksdagen_document_types[self.riksdagen_dataset_title][
            "filename"
        ]

    @property
    def dataset_wikidata_qid(self) -> str:
        return config.supported_riksdagen_document_types[self.riksdagen_dataset_title][
            "wikidata_qid"
        ]

    def get_dataset_id(self):
        self.dataset_id = self.database_handler.get_dataset_id(dataset_handler=self)
        if not self.dataset_id:
            self.database_handler.insert_dataset_in_database(dataset_handler=self)
            self.dataset_id = self.database_handler.get_dataset_id(dataset_handler=self)

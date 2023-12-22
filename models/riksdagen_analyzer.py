import argparse
import logging
from typing import List

from pandas import DataFrame
from pydantic import BaseModel

from models.crud.create import Create
from models.crud.database_handler import Mariadb
from models.datasets import Datasets
from models.riksdagen_document import RiksdagenDocument

logger = logging.getLogger(__name__)


class RiksdagenAnalyzer(BaseModel):
    """This model extracts sentences from a supported riksdagen document type
    and stores the result in a jsonl format."""

    riksdagen_dataset_title: str = ""
    documents: List[RiksdagenDocument] = []
    df: DataFrame = DataFrame()
    max_documents_to_extract: int = 0  # zero means no limit
    max_datasets_to_extract: int = 0   # zero means no limit
    skipped_documents_count: int = 0
    # document_offset: int = 0
    token_count: int = 0
    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    jsonl_path_to_load: str = ""
    arguments: argparse.Namespace = argparse.Namespace()
    mariadb: Mariadb = Mariadb()
    datasets: Datasets = Datasets()

    class Config:
        arbitrary_types_allowed = True

    def start(self):
        self.setup_database()
        self.setup_datasets()
        self.iterate_datasets()

    def iterate_datasets(self):
        count = 0
        for dataset in self.datasets.datasets:
            if self.max_datasets_to_extract and count <= self.max_datasets_to_extract:
                dataset.max_documents_to_extract_per_dataset = self.max_documents_to_extract
                # dataset.document_offset = self.document_offset
                dataset.read_json_from_disk_and_extract()
                dataset.print_number_of_skipped_documents()
                dataset.print_number_of_tokens()
                count += 1
            else:
                print("Max number of datasets reached")

    def setup_database(self):
        create = Create()
        create.connect_and_setup()
        create.close_db()

    def setup_datasets(self):
        self.datasets = Datasets(analyzer=self)
        self.datasets.setup()

    def handle_arguments(self):
        self.setup_argparse()
        self.arguments = self.parser.parse_args()
        if self.arguments.max_documents:
            self.max_documents_to_extract = self.arguments.max_documents
        if self.arguments.max_datasets:
            self.max_datasets_to_extract = self.arguments.max_datasets
        self.start()

    def print_number_of_skipped_documents(self):
        print(
            f"Number of skipped JSON files "
            f"(because of missing or bad data): {self.skipped_documents_count}"
        )

    def setup_argparse(self):
        self.parser = argparse.ArgumentParser(
            description="Analyze all open data from Riksdagen"
        )
        self.parser.add_argument(
            "--max-documents", type=int, help="Max number of documents to process per dataset", required=False
        )
        self.parser.add_argument(
            "--max-datasets", type=int, help="Max number of datasets to process", required=False
        )

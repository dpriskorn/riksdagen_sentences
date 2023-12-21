import argparse
import json
import logging
import os
from typing import List

from pandas import DataFrame
from pydantic import BaseModel
from tqdm import tqdm

from models.database_handler import DatabaseHandler
from models.dataset_handler import DatasetHandler
from models.riksdagen_document import RiksdagenDocument

logger = logging.getLogger(__name__)


class RiksdagenAnalyzer(BaseModel):
    """This model extracts sentences from a supported riksdagen document type
    and stores the result in a jsonl format."""

    riksdagen_dataset_title: str = ""
    documents: List[RiksdagenDocument] = []
    df: DataFrame = DataFrame()
    max_documents_to_extract: int = 0  # zero means no limit
    skipped_documents_count: int = 0
    document_offset: int = 0
    token_count: int = 0
    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    jsonl_path_to_load: str = ""
    arguments: argparse.Namespace = argparse.Namespace()
    database_handler: DatabaseHandler = DatabaseHandler()
    dataset_handler: DatasetHandler = DatasetHandler()

    class Config:
        arbitrary_types_allowed = True

    def start(self):
        self.database_handler.connect_and_setup()
        self.setup_dataset()
        if self.dataset_handler.dataset_id:
            self.read_json_from_disk_and_extract()
            self.print_number_of_skipped_documents()
            self.print_number_of_tokens()
        # self.generate_document_term_matix()

    def setup_dataset(self):
        self.dataset_handler.database_handler = self.database_handler
        self.dataset_handler.riksdagen_dataset_title = self.riksdagen_dataset_title
        self.dataset_handler.get_dataset_id()

    def handle_arguments(self):
        self.setup_argparse()
        self.arguments = self.parser.parse_args()
        if self.arguments.max:
            self.max_documents_to_extract = self.arguments.max
        if self.arguments.offset:
            self.document_offset = self.arguments.offset
        if self.arguments.analyze:
            self.riksdagen_dataset_title = self.arguments.analyze
            self.start()

    def print_number_of_skipped_documents(self):
        print(
            f"Number of skipped JSON files "
            f"(because of missing or bad data): {self.skipped_documents_count}"
        )

    # def detect_language_for_all_sentences(self):
    #     print("Determining language for suitable sentences")
    #     suitable_sentences = self.df[self.df["suitable"]]["sentence"]
    #
    #     # Use tqdm to show progress while applying language detection
    #     for idx in tqdm(
    #         suitable_sentences.index,
    #         desc="Detecting language",
    #         total=len(suitable_sentences),
    #     ):
    #         language_detection_result = self.detect_language(
    #             self.df.at[idx, "sentence"]
    #         )
    #         self.df.at[idx, "lang"] = language_detection_result["lang"]
    #         self.df.at[idx, "score"] = language_detection_result["score"]

    # def determine_suitability(self):
    #     print("determining suitability")
    #     # Apply the suitable_sentence function to the 'sentences' column
    #     self.df["suitable"] = self.df["sentence"].apply(self.suitable_sentence)

    # def strip_newlines(self):
    #     # Remove newlines from the end of sentences in the 'sentences' column
    #     self.df["sentence"] = self.df["sentence"].astype(str).str.rstrip("\n")

    # def save(self):
    #     # self.df.to_pickle(f"{self.filename}.pickle.xz", compression="xz")
    #     # self.df.to_csv(f"{self.filename}.csv.xz", compression="xz")
    #     self.append_suitable_sentences_to_jsonl()

    # def create_dataframe_with_all_sentences(self):
    #     print("creating dataframe")
    #     # Creating DataFrame
    #     data = {"id": [], "sentence": [], "tokens": 0}
    #
    #     for doc in self.documents:
    #         for sentence in doc.sentences:
    #             data["id"].append(doc.id)
    #             data["sentence"].append(sentence.text)
    #             data["tokens"] = sentence.token_count
    #
    #     self.df = pd.DataFrame(data)

    # def dataframe_is_empty(self) -> bool:
    #     return self.df.empty

    def read_json_from_disk_and_extract(self):
        logger.info("reading json from disk")
        file_paths = []
        for root, dirs, files in os.walk(self.dataset_handler.workdirectory):
            for file in files:
                if file.endswith(".json"):
                    file_paths.append(os.path.join(root, file))

        logger.info(f"Number of filepaths found: {len(file_paths)}")

        # Handle offset
        file_paths = file_paths[self.document_offset :]
        # print(file_paths[:1])
        # exit()
        logger.info(f"Number of filepaths after offset: {len(file_paths)}")

        # Wrap the iteration with tqdm to display a progress bar
        count = 0
        for file_path in tqdm(file_paths, desc="Processing JSON files"):
            # Only break if max_documents_to_extract is different from 0
            if self.max_documents_to_extract and count >= self.max_documents_to_extract:
                logger.info("Max documents limit reached.")
                break
            with open(file_path, "r", encoding="utf-8-sig") as json_file:
                try:
                    data = json.load(json_file)
                    if (
                        "dokumentstatus" in data
                        and "dokument" in data["dokumentstatus"]
                    ):
                        dok_id = data["dokumentstatus"]["dokument"].get("dok_id")
                        text = data["dokumentstatus"]["dokument"].get("text")
                        html = data["dokumentstatus"]["dokument"].get("html")

                        if dok_id is not None and (
                            text is not None or html is not None
                        ):
                            # We got a good document with content
                            document = RiksdagenDocument(
                                external_id=dok_id,
                                dataset_id=self.dataset_handler.dataset_id,
                                text=text or "",
                                html=html or "",
                                database_handler=self.database_handler,
                            )
                            self.database_handler.add_document_to_database(
                                document=document
                            )
                            document.extract_sentences()
                            # self.token_count = +document.token_count
                            # self.print_number_of_tokens()
                            self.documents.append(document)
                            # self.create_dataframe_with_all_sentences()
                            if not self.dataframe_is_empty():
                                self.generate_uuid()
                                self.strip_newlines()
                                self.determine_suitability()
                                self.detect_language_for_all_sentences()
                                self.append_suitable_sentences_to_jsonl()
                            else:
                                logger.warning(
                                    f"Document with id {document.external_id} with path "
                                    f"{file_path} did not have any sentences"
                                )
                                self.skipped_documents_count += 1
                            # Reset documents to avoid getting killed by the
                            # kernel because we run out of memory
                            self.documents = []
                        else:
                            self.skipped_documents_count += 1
                            logger.info(
                                f"Skipping document {json_file}: Missing dok_id and (text or html)"
                            )
                    else:
                        logger.info(
                            f"Skipping document {json_file}: Missing 'dokumentstatus' or 'dokument'"
                        )
                except json.JSONDecodeError as e:
                    logger.error(f"Error loading JSON from {file_path}: {e}")
                count = +1

    def print_number_of_documents(self):
        # Print or use the variable containing all text
        print(f"number of documents: {len(self.documents)}")

    def print_number_of_tokens(self):
        # Print or use the variable containing all text
        print(f"Total number of tokens: {self.token_count}")

    def setup_argparse(self):
        self.parser = argparse.ArgumentParser(
            description="Parse open data from Riksdagen"
        )
        # self.parser.add_argument(
        #     "-l", "--load-jsonl", type=str, help="Load JSONL file", required=False
        # )
        self.parser.add_argument(
            "--offset", type=int, help="Document offset", required=False
        )
        self.parser.add_argument(
            "--max", type=int, help="Max documents to process", required=False
        )
        self.parser.add_argument(
            "--analyze",
            type=str,
            help="Analyze a document series. One of ['departementserien', 'proposition']",
            required=True,
        )

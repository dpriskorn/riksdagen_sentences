import argparse
import json
import logging
import os
import sqlite3
import string
import uuid
from sqlite3 import Cursor
from typing import List, Dict, Any

import pandas as pd
import yaml
from ftlangdetect import detect
from pandas import DataFrame
from pydantic import BaseModel
from tqdm import tqdm

import config
from models.riksdagen_document import RiksdagenDocument

logger = logging.getLogger(__name__)


class RiksdagenAnalyzer(BaseModel):
    """This model extracts sentences from a supported riksdagen document type
    and stores the result in a jsonl format."""

    riksdagen_document_type: str = ""
    documents: List[RiksdagenDocument] = []
    df: DataFrame = DataFrame()
    max_documents_to_extract: int = 0  # zero means no limit
    skipped_documents_count: int = 0
    document_offset: int = 0
    token_count: int = 0
    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    jsonl_path_to_load: str = ""
    arguments: argparse.Namespace = argparse.Namespace()
    languages: Dict[Any, Any] = dict()
    language_config_path: str = "config/languages.yml"
    connection: Any = None
    tuple_cursor: Cursor = None
    row_cursor: Cursor = None

    class Config:
        arbitrary_types_allowed = True

    @property
    def workdirectory(self) -> str:
        return config.supported_riksdagen_document_types[self.riksdagen_document_type][
            "workdirectory"
        ]

    @property
    def filename(self):
        return config.supported_riksdagen_document_types[self.riksdagen_document_type][
            "filename"
        ]

    def start(self):
        self.load_languages_from_yaml()
        self.connect_to_db()
        self.initialize_cursors()
        self.create_tables()
        self.create_indexes()
        self.insert_langauges_from_yaml()
        self.commit_and_close_db()
        exit()
        self.read_json_from_disk_and_extract()
        self.print_number_of_skipped_documents()
        self.print_number_of_tokens()
        # self.generate_document_term_matix()

    def load_languages_from_yaml(self):
        # Load YAML into a dictionary
        with open(self.language_config_path, 'r') as file:
            # Read YAML content from the file
            self.languages = yaml.safe_load(file)

    def insert_langauges_from_yaml(self):
        print("Inserting languages from YAML")
        # Construct the SQL INSERT query
        query = '''
                INSERT OR IGNORE INTO language (name_en, iso_code, qid)
                VALUES (?, ?, ?)
                '''

        # Iterate through each language and insert its data
        for lang_code, lang_data in self.languages['development'].items():
            language_name_en = lang_data['language_name_en']
            iso_code = lang_code
            qid = lang_data['language_qid']

            # Execute the query with the extracted values for each language
            self.tuple_cursor.execute(query, (language_name_en, iso_code, qid))

    def handle_arguments(self):
        self.setup_argparse()
        self.arguments = self.parser.parse_args()
        if self.arguments.max:
            self.max_documents_to_extract = self.arguments.max
        if self.arguments.offset:
            self.document_offset = self.arguments.offset
        if self.arguments.analyze:
            self.riksdagen_document_type = self.arguments.analyze
            self.start()

    def print_number_of_skipped_documents(self):
        print(
            f"Number of skipped JSON files "
            f"(because of missing or bad data): {self.skipped_documents_count}"
        )

    @staticmethod
    def detect_language(text) -> Dict[str, float]:
        # This returns a dict like so: {'lang': 'tr', 'score': 0.9982126951217651}
        return detect(text=text.replace("\n", ""), low_memory=False)

    def detect_language_for_all_sentences(self):
        print("Determining language for suitable sentences")
        suitable_sentences = self.df[self.df["suitable"]]["sentence"]

        # Use tqdm to show progress while applying language detection
        for idx in tqdm(
            suitable_sentences.index,
            desc="Detecting language",
            total=len(suitable_sentences),
        ):
            language_detection_result = self.detect_language(
                self.df.at[idx, "sentence"]
            )
            self.df.at[idx, "lang"] = language_detection_result["lang"]
            self.df.at[idx, "score"] = language_detection_result["score"]

    @staticmethod
    def suitable_sentence(sentence):
        # Removing punctuation
        sentence_without_punctuation = "".join(
            char for char in sentence if char not in string.punctuation
        )

        # Split the sentence into words and remove words containing numbers
        words = [
            word
            for word in sentence_without_punctuation.split()
            if not any(char.isdigit() for char in word)
        ]

        # Check if the sentence has more than 5 words after removing numeric words
        if len(words) > 5:
            return True
        else:
            return False

    def determine_suitability(self):
        print("determining suitability")
        # Apply the suitable_sentence function to the 'sentences' column
        self.df["suitable"] = self.df["sentence"].apply(self.suitable_sentence)

    def strip_newlines(self):
        # Remove newlines from the end of sentences in the 'sentences' column
        self.df["sentence"] = self.df["sentence"].astype(str).str.rstrip("\n")

    def save(self):
        # self.df.to_pickle(f"{self.filename}.pickle.xz", compression="xz")
        # self.df.to_csv(f"{self.filename}.csv.xz", compression="xz")
        self.append_suitable_sentences_to_jsonl()

    def generate_uuid(self):
        # Generate UUIDs for each sentence and add them to a new 'uuid' column
        self.df["uuid"] = [str(uuid.uuid4()) for _ in range(len(self.df))]

    def create_dataframe_with_all_sentences(self):
        print("creating dataframe")
        # Creating DataFrame
        data = {"id": [], "sentence": [], "tokens": 0}

        for doc in self.documents:
            for sentence in doc.sentences:
                data["id"].append(doc.id)
                data["sentence"].append(sentence.text)
                data["tokens"] = sentence.token_count

        self.df = pd.DataFrame(data)

    def dataframe_is_empty(self) -> bool:
        return self.df.empty

    def read_json_from_disk_and_extract(self):
        # print("reading json from disk")
        file_paths = []
        for root, dirs, files in os.walk(self.workdirectory):
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
                            document = RiksdagenDocument(
                                id=dok_id, text=text or "", html=html or ""
                            )
                            document.extract_sentences()
                            self.token_count = +document.token_count
                            self.print_number_of_tokens()
                            self.documents.append(document)
                            self.create_dataframe_with_all_sentences()
                            if not self.dataframe_is_empty():
                                self.generate_uuid()
                                self.strip_newlines()
                                self.determine_suitability()
                                self.detect_language_for_all_sentences()
                                self.append_suitable_sentences_to_jsonl()
                            else:
                                logger.warning(
                                    f"Document with id {document.id} with path "
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
        self.parser = argparse.ArgumentParser(description="Parse open data from Riksdagen")
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

    @staticmethod
    def item_int(qid) -> int:
        return int(qid[1:])

    def connect_to_db(self) -> None:
        db_file = "database.db"
        # Connect to the database
        self.connection = sqlite3.connect(db_file)

    def initialize_cursors(self) -> None:
        # Create cursors to interact with the database
        self.row_cursor = self.connection.cursor()
        self.row_cursor.row_factory = sqlite3.Row
        self.tuple_cursor = self.connection.cursor()
        self.tuple_cursor.row_factory = None

    def create_tables(self):
        logger.info("Creating tables")
        sql_queries = [
            '''CREATE TABLE IF NOT EXISTS language (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name_en TEXT NOT NULL UNIQUE,
                iso_code TEXT NOT NULL UNIQUE,
                qid TEXT NOT NULL UNIQUE
            );''',

            '''CREATE TABLE IF NOT EXISTS provider (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                qid TEXT NOT NULL
            );''',

            '''CREATE TABLE IF NOT EXISTS collection (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                qid TEXT NOT NULL,
                provider INT NOT NULL,
                FOREIGN KEY (provider) REFERENCES provider(id)
            );''',

            '''CREATE TABLE IF NOT EXISTS dataset (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                qid TEXT NOT NULL,
                collection INT NOT NULL,
                FOREIGN KEY (collection) REFERENCES collection(id)
            );''',

            '''CREATE TABLE IF NOT EXISTS document (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset INT NOT NULL,
                external_id TEXT NOT NULL,
                FOREIGN KEY (dataset) REFERENCES dataset(id)
            );''',

            '''CREATE TABLE IF NOT EXISTS sentence (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                text TEXT NOT NULL,
                uuid TEXT NOT NULL UNIQUE,
                document INT NOT NULL,
                language INT NOT NULL,
                score FLOAT NOT NULL,
                token_count INT NOT NULL,
                FOREIGN KEY (document) REFERENCES document(id),
                FOREIGN KEY (language) REFERENCES language(id)
            );'''
        ]
        for query in sql_queries:
            self.tuple_cursor.execute(query)

    def create_indexes(self):
        """These indexes enable us to fast lookup of sentences
        in a given language, document or with a given UUID"""
        logger.info("Creating indexes")
        sql_index_queries = [
            "CREATE INDEX IF NOT EXISTS idx_language_id ON language(id);",
            "CREATE INDEX IF NOT EXISTS idx_provider_id ON provider(id);",
            "CREATE INDEX IF NOT EXISTS idx_collection_id ON collection(id);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_id ON dataset(id);",
            "CREATE INDEX IF NOT EXISTS idx_document_id ON document(id);",
            "CREATE INDEX IF NOT EXISTS idx_sentence_id ON sentence(id);",
            "CREATE INDEX IF NOT EXISTS idx_sentence_uuid ON sentence(uuid);",
            "CREATE INDEX IF NOT EXISTS idx_sentence_document_id ON sentence(document);",
            "CREATE INDEX IF NOT EXISTS idx_sentence_language ON sentence(language);",
        ]
        for query in sql_index_queries:
            self.tuple_cursor.execute(query)

    def commit_and_close_db(self) -> None:
        # Don't forget to close the connection when done
        self.connection.commit()
        self.connection.close()

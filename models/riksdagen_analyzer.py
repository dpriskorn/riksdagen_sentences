import argparse
import hashlib
import json
import logging
import lzma
import os
import sqlite3
import string
import uuid
from typing import List, Dict

import jsonlines
from ftlangdetect import detect
from pandas import DataFrame
from pydantic import BaseModel
import pandas as pd
from tqdm import tqdm

import config
from models.RiksdagenJsonl import RiksdagenJsonFileProcessor
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

    def start_analyzing(self):
        self.read_json_from_disk_and_extract()
        self.print_number_of_skipped_documents()
        self.print_number_of_tokens()

    def handle_arguments(self):
        self.setup_argparse()
        self.arguments = self.parser.parse_args()
        if self.arguments.load_jsonl:
            json_processor = RiksdagenJsonFileProcessor(
                file_path=self.arguments.load_jsonl
            )
            json_processor.process_json()
        if self.arguments.max:
            self.max_documents_to_extract = self.arguments.max
        if self.arguments.offset:
            self.document_offset = self.arguments.offset
        if self.arguments.analyze:
            self.riksdagen_document_type = self.arguments.analyze
            self.start_analyzing()

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

    def print_statistics(self):
        print("Statistics:")
        # Counting total number of rows
        total_sentences = self.df.shape[0]
        print("Total number of sentences:", total_sentences)

        # Count the number of empty sentences
        empty_sentences_count = (self.df["sentence"].str.strip() == "").sum()
        print(f"Number of empty sentences: {empty_sentences_count}")

        # Counting rows where 'suitable' column is True
        suitable_count = self.df[self.df["suitable"] == True].shape[0]
        print("Number of suitable sentences:", suitable_count)
        suitable_percentage = (suitable_count / total_sentences) * 100
        print("Percentage suitable sentences:", round(suitable_percentage))

        # Tokens
        try:
            total_tokens = self.df["tokens"].sum()
            print("Total sum of 'tokens' column:", total_tokens)
            # Calculate the mean number of tokens per sentence
            mean_tokens_per_sentence = total_tokens / total_sentences
            print("Mean number of tokens per sentence:", mean_tokens_per_sentence)
        except KeyError:
            pass

        # Counting and displaying the distribution of language codes
        language_distribution = self.df["langdetect"].value_counts()
        # Calculating percentages
        try:
            en_percentage = (language_distribution["en"] / total_sentences) * 100
            sv_percentage = (language_distribution["sv"] / total_sentences) * 100
            print("Percentage of 'en' (English) sentences:", round(en_percentage))
            print("Percentage of 'sv' (Swedish) sentences:", round(sv_percentage))
            print("Language code distribution:")
            print(language_distribution)
        except KeyError:
            pass

    def save(self):
        # self.df.to_pickle(f"{self.filename}.pickle.xz", compression="xz")
        # self.df.to_csv(f"{self.filename}.csv.xz", compression="xz")
        self.append_suitable_sentences_to_jsonl()

    def append_suitable_sentences_to_jsonl(self):
        # Filter the DataFrame where 'suitable' column is True
        filtered_df = self.df[self.df["suitable"]]
        df_without_suitable = filtered_df.drop("suitable", axis=1)

        # Convert DataFrame to list of dictionaries
        data = df_without_suitable.to_dict(orient="records")

        filename = f"{self.filename}.jsonl"

        # Write data to a JSONL file
        with jsonlines.open(filename, mode="a") as writer:
            writer.write_all(data)

    def compress_jsonl(self):
        # Compress the JSONL file with xz
        with open(f"{self.filename}.jsonl", "rb") as jsonl_file:
            with lzma.open(f"{self.filename}.jsonl.xz", "wb") as xz_file:
                for line in jsonl_file:
                    xz_file.write(line)

    # def remove_uncompressed_jsonl(self):
    #     os.remove(self.filename)

    @staticmethod
    def generate_md5_hash(sentence):
        return hashlib.md5(sentence.encode()).hexdigest()

    def generate_id_and_hash(self):
        # Generate UUIDs for each sentence and add them to a new 'uuid' column
        self.df["uuid"] = [str(uuid.uuid4()) for _ in range(len(self.df))]

        # Generate MD5 hash for each sentence and add them to a new 'md5_hash' column

        self.df["md5_hash"] = self.df["sentence"].apply(self.generate_md5_hash)

    def create_dataframe_with_all_sentences(self):
        print("creating dataframe")
        # Creating DataFrame
        data = {"id": [], "sentence": [], "tokens": 0}

        for doc in self.documents:
            for sentence in doc.sentences:
                data["id"].append(doc.id)
                data["sent"].append(sentence.text)
                data["tok"] = sentence.token_count
                data["ent"] = sentence.entities

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
                                self.generate_id_and_hash()
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
        self.parser = argparse.ArgumentParser(description="Parse JSONL file")
        self.parser.add_argument(
            "-l", "--load-jsonl", type=str, help="Load JSONL file", required=False
        )
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
            required=False,
        )

    @staticmethod
    def item_id(entity) -> int:
        return int(entity.entity_id[1:])

    def check_table(self) -> None:
        sql_query = "PRAGMA table_info(joined);"
        self.tuple_cursor.execute(sql_query)
        result = self.tuple_cursor.fetchall()
        print(result)

    def connect_to_db(self) -> None:
        db_file = 'database.db'
        # Connect to the database
        self.connection = sqlite3.connect(db_file)

    def initialize_cursors(self) -> None:
        # Create cursors to interact with the database
        self.row_cursor = self.connection.cursor()
        self.row_cursor.row_factory = sqlite3.Row
        self.tuple_cursor = self.connection.cursor()
        self.tuple_cursor.row_factory = None



    def create_indexes(self):
        logger.info("Creating indexes")
        query1 = "CREATE INDEX IF NOT EXISTS idx_processed ON joined (processed);"
        self.tuple_cursor.execute(query1)

    def commit_and_close_db(self) -> None:
        # Don't forget to close the connection when done
        # self.conn.commit()
        self.connection.close()

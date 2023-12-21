import logging
import re
import sqlite3
from sqlite3 import Cursor, DatabaseError
from typing import List, Any

import spacy
from bs4 import BeautifulSoup
from pydantic import BaseModel

from models.database_handler import DatabaseHandler
from models.sentence import Sentence

logger = logging.getLogger(__name__)


class RiksdagenDocument(BaseModel):
    """This model supports extraction of sentences based on html or text input
    It uses spaCy to find sentence boundaries

    We could support storing title for documents,
    but it is not essential so we skip it for now"""

    id: str
    dataset_id: int
    text: str = ""
    html: str = ""
    chunk_size: int = 100000
    chunks: List[str] = list()
    sentences: List[Sentence] = list()
    connection: Any = None
    tuple_cursor: Cursor = None
    row_cursor: Cursor = None
    nlp: Any = None
    database_handler: DatabaseHandler

    class Config:
        arbitrary_types_allowed = True

    @property
    def token_count(self) -> int:
        return sum([sent.token_count for sent in self.sentences])

    @property
    def count_words(self) -> int:
        # Counting words in the text
        return len(self.text.split())

    @property
    def number_of_chunks(self) -> int:
        # Count the number of chunks
        return len(self.chunks)

    @property
    def number_of_sentences(self) -> int:
        # Count the number of chunks
        return len(self.sentences)

    # def connect_to_db(self) -> None:
    #     db_file = "database.db"
    #     # Connect to the database
    #     self.connection = sqlite3.connect(db_file)
    #
    # def initialize_cursors(self) -> None:
    #     # Create cursors to interact with the database
    #     self.row_cursor = self.connection.cursor()
    #     self.row_cursor.row_factory = sqlite3.Row
    #     self.tuple_cursor = self.connection.cursor()
    #     self.tuple_cursor.row_factory = None
    #
    # def commit_to_database(self) -> None:
    #     # Don't forget to close the connection when done
    #     self.connection.commit()
    #
    # def close_db(self) -> None:
    #     self.connection.close()
    #
    def chunk_text(self):
        # Function to chunk the text
        start = 0
        while start < len(self.text):
            self.chunks.append(self.text[start : start + self.chunk_size])
            start += self.chunk_size

    def convert_html_to_text(self):
        # Check if HTML content exists for the document
        soup = BeautifulSoup(self.html, "lxml")
        # Extract text from the HTML
        # TODO investigate how stripping affects the result
        self.text = soup.get_text(separator=" ", strip=False)

    def print_number_of_chunks(self):
        # Display the number of chunks
        logger.info(f"Number of chunks: " f"{self.number_of_chunks}")

    def extract_sentences(self):
        if not self.text:
            # We assume html is present
            self.convert_html_to_text()

        # Load the Swedish language model
        self.nlp = spacy.load("sv_core_news_lg")

        # Displaying the word count
        logger.info(f"Number of words before tokenization: " f"{self.count_words}")

        # Chunk the text
        self.chunk_text()
        self.print_number_of_chunks()
        self.iterate_chunks()

    def print_number_of_sentences(self):
        logger.info(f"Extracted {len(self.sentences)} sentences")

    def iterate_chunks(self):
        # Process each chunk separately
        count = 1
        for chunk in self.chunks:
            logger.info(f"loading chunk {count}/" f"{self.number_of_chunks}")
            doc = self.nlp(chunk)
            self.iterate_sentences(doc=doc)
            count += 1

    def iterate_sentences(self, doc: Any):
        for sent in doc.sents:
            sentence = Sentence(sent=sent, database_handler=self.database_handler)
            sentence.start()
            # exit()
            # filtered_sentences = [
            #     Sentence(text=sent.text) for sent in doc.sents if sent.text.strip()
            # ]
            # self.sentences.extend(filtered_sentences)


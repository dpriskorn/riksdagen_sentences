import logging
import re
import sqlite3
from sqlite3 import Cursor
from typing import List, Any

import spacy
from bs4 import BeautifulSoup
from pydantic import BaseModel

from models.sentence import Sentence

logger = logging.getLogger(__name__)


class RiksdagenDocument(BaseModel):
    """This model supports extraction of sentences based on html or text input
    It uses spaCy to find sentence boundaries"""

    id: str
    text: str = ""
    html: str = ""
    chunk_size: int = 100000
    chunks: List[str] = list()
    sentences: List[Sentence] = list()
    connection: Any = None
    tuple_cursor: Cursor = None
    row_cursor: Cursor = None

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

    def commit_and_close_db(self) -> None:
        # Don't forget to close the connection when done
        # self.conn.commit()
        self.connection.close()

    def add_document_to_database(self):
        self.connect_to_db()
        self.initialize_cursors()
        # Assuming 'documents' is the name of the table where documents are stored
        query = "INSERT INTO document (external_id, dataset) VALUES (?, ?, ?)"
        values = (self.id, self.text, self.html)

        try:
            self.row_cursor.execute(query, values)
            self.connection.commit()
            print("Document added to the database.")
        except sqlite3.Error as e:
            print(f"Error adding document to the database: {e}")
        finally:
            self.commit_and_close_db()

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

    def extract_sentences(self):
        if not self.text:
            # We assume html is present
            self.convert_html_to_text()

        # Load the Swedish language model
        nlp = spacy.load("sv_core_news_lg")

        # Displaying the word count
        logger.info(f"Number of words before tokenization: {self.count_words}")

        # Chunk the text
        self.chunk_text()

        # Display the number of chunks
        logger.info(f"Number of chunks: {self.number_of_chunks}")

        # Process each chunk separately
        count = 1
        for chunk in self.chunks:
            logger.info(f"loading chunk {count}/{self.number_of_chunks}")
            doc = nlp(chunk)

            # Debugging sent.text.split()
            for sent in doc.sents:
                # Remove newlines, digits and a selection of characters
                cleaned_sentence = re.sub(
                    r"\d+",
                    "",
                    sent.text.replace("\n", "")
                    .replace("\r", "")
                    .replace(":", "")
                    .replace(",", "")
                    .replace(".", "")
                    .replace("(", "")
                    .replace(")", "")
                    .replace("-", "")
                    .replace("–", "")
                    .replace("/", "")
                    .strip(),
                )
                token_count = len(cleaned_sentence.split())

                logger.debug(
                    "Sentence text: %s, Split: %s", cleaned_sentence, token_count
                )
                filtered_sentences = [
                    Sentence(text=sent.text, token_count=token_count)
                    for sent in doc.sents
                    if sent.text.strip()
                ]
                self.sentences.extend(filtered_sentences)
            count += 1

        logger.info(f"Extracted {len(self.sentences)} sentences")

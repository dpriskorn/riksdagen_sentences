import logging
from typing import List, Any

import spacy
from bs4 import BeautifulSoup
from pydantic import BaseModel

from models.crud.insert import Insert
from models.crud.read import Read
from models.crud.update import Update
from models.sentence import Sentence

logger = logging.getLogger(__name__)


class Document(BaseModel):
    """This model supports extraction of sentences based on html or text input
    It uses spaCy to find sentence boundaries

    We could support storing title for documents,
    but it is not essential so we skip it for now"""

    external_id: str
    dataset_id: int
    text: str = ""
    html: str = ""
    chunk_size: int = 100000  # this is because of a spacy limitation
    chunks: List[str] = list()
    accepted_sentences: List[Sentence] = list()
    nlp: Any = None

    class Config:
        arbitrary_types_allowed = True

    @property
    def id(self) -> int:
        """ID of this document in the database"""
        read = Read()
        read.connect_and_setup()
        data = read.get_document_id(document=self)
        read.close_db()
        return data

    def already_processed(self) -> bool:
        read = Read()
        read.connect_and_setup()
        data = read.get_processed_status(document=self)
        read.close_db()
        return data

    # @property
    # def token_count(self) -> int:
    #     return sum([sent.token_count for sent in self.sentences])

    @property
    def count_words(self) -> int:
        # Counting words in the text
        return len(self.text.split())

    @property
    def number_of_chunks(self) -> int:
        # Count the number of chunks
        return len(self.chunks)

    @property
    def number_of_accepted_sentences(self) -> int:
        # Count the number of chunks
        return len(self.accepted_sentences)

    @property
    def number_of_accepted_tokens(self):
        return sum(
            [len(sentence.accepted_tokens) for sentence in self.accepted_sentences]
        )

    @property
    def text_length(self) -> int:
        return len(self.text)

    def chunk_text(self):
        """Function to chunk the text without splitting sentences"""
        text_length = self.text_length
        if text_length > self.chunk_size:
            start = 0
            while start < text_length:
                # Find the end point ensuring it's a full stop
                end = start + self.chunk_size

                # Check if the end is within the text length
                if end < text_length:
                    # If the character at 'end' is not a full stop and we're not at the start,
                    # keep moving 'end' backwards until a full stop is found or until reaching 'start'
                    while self.text[end] != "." and end > start:
                        end -= 1

                # If there's no full stop found within the chunk size, chunk until self.chunk_size
                if end == start:
                    end = start + self.chunk_size

                # Add the chunk to the list
                self.chunks.append(self.text[start:end])

                # Update 'start' for the next iteration, move beyond the last full stop found
                start = end + 1 if end + 1 < text_length else text_length
        else:
            # Append the whole text
            self.chunks.append(self.text)

    def convert_html_to_text(self):
        # todo remove tables before conversion
        # Check if HTML content exists for the document
        soup = BeautifulSoup(self.html, "lxml")
        # Extract text from the HTML
        # TODO investigate how stripping affects the result
        self.text = soup.get_text(separator=" ", strip=False)

    # def print_number_of_chunks(self):
    #     # Display the number of chunks
    #     logger.info(f"Number of chunks: " f"{self.number_of_chunks}")

    def extract_sentences(self):
        if not self.already_processed():
            if not self.text:
                # We assume html is present
                self.convert_html_to_text()
            if self.text:
                print(
                    f"Extracting document {self.external_id} with {self.count_words} words"
                )
                # Load the Swedish language model
                self.nlp = spacy.load("sv_core_news_lg")
                self.chunk_text()
                # todo insert chunk md5 in the database
                # self.print_number_of_chunks()
                self.iterate_chunks()
        else:
            logger.info(f"Skipping already processed document {self.external_id}")

    # def print_number_of_sentences(self):
    #     logger.info(f"Extracted {len(self.accepted_sentences)} sentences")

    def iterate_chunks(self):
        count = 1
        for chunk in self.chunks:
            print(f"Iterating chunk {count}/" f"{self.number_of_chunks}")
            # todo check if chunk md5 has been processed
            doc = self.nlp(chunk)
            self.iterate_sentences(doc=doc)
            count += 1
        print(
            f"Found {self.number_of_accepted_sentences} "
            f"accepted sentences with a total of {self.number_of_accepted_tokens}"
        )

    def iterate_sentences(self, doc: Any):
        sentence_count = 0
        for _ in doc.sents:
            sentence_count += 1
        print(f"Iterating {sentence_count} sentences in this chunk")
        count = 1
        for sent in doc.sents:
            if count % 100 == 0 or count == 1:
                print(f"Iterating sentence {count}/{sentence_count}")
            sentence = Sentence(sent=sent, document=self)
            sentence.analyze_and_insert()
            self.accepted_sentences.append(sentence)
            count += 1

    def insert_extract_and_update(self):
        if not self.id:
            self.insert_document()
        self.extract_sentences()
        self.update_document()

    def insert_document(self):
        insert = Insert()
        insert.connect_and_setup()
        insert.insert_document(document=self)
        insert.close_db()

    def update_document(self):
        update = Update()
        update.connect_and_setup()
        update.update_document_as_processed(document=self)
        update.close_db()

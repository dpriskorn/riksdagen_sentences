import logging
from typing import List, Any

import spacy
from spacy.language import Doc
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

    @property
    def equivalent_pages(self) -> int:
        """We estimate that a standard A4 page has 450 words"""
        return int(self.count_words / 450)

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
                    f"Extracting document {self.external_id} with "
                    f"{self.count_words} words which equals "
                    f"{self.equivalent_pages} A4 pages"
                )
                # Load the Swedish language model
                self.nlp = spacy.load("sv_core_news_lg")
                # The senter is 10x faster than the parser and we don't need dependency parsing
                # See https://spacy.io/models/
                self.nlp.disable_pipe("parser")
                self.nlp.enable_pipe("senter")
                self.chunk_text()
                # self.print_number_of_chunks()
                self.iterate_chunks()
        else:
            logger.info(f"Skipping already processed document {self.external_id}")

    # def print_number_of_sentences(self):
    #     logger.info(f"Extracted {len(self.accepted_sentences)} sentences")

    @staticmethod
    def clean_toc(chunk: str):
        """We clean away sentences with more than three
        full stops in a row because they are just
        referring to headings found further down in the document
        TOC= table of contents"""
        lines = chunk.split("\n")
        cleaned_lines = []

        for line in lines:
            if line.count("....") == 0:
                cleaned_lines.append(line)
            else:
                pass
                # logger.debug(f"Discarded line: '{line}'")

        cleaned_chunk = "\n".join(cleaned_lines)
        return cleaned_chunk

    def iterate_chunks(self):
        count = 1
        for chunk in self.chunks:
            chunk = self.clean_toc(chunk=chunk)
            # print(chunk[:10000])
            # exit()
            print(f"Iterating chunk {count}/" f"{self.number_of_chunks}")
            doc = self.nlp(chunk)
            self.iterate_sentences(doc=doc)
            count += 1
        print(
            f"Found {self.number_of_accepted_sentences} "
            f"accepted sentences with a total of {self.number_of_accepted_tokens}"
        )

    def iterate_sentences(self, doc: Doc):
        sentence_count = 0
        for _ in doc.sents:
            sentence_count += 1
        print(f"Iterating {sentence_count} sentences in this chunk")
        count = 1
        for sent in doc.sents:
            if count % 100 == 0 or count == 1:
                print(f"Iterating sentence {count}/{sentence_count}")
            sentence = Sentence(doc=doc, sent=sent, document=self)
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

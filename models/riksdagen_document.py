import logging
import re
from typing import List

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
        """Extract sentences and do the entity linking"""
        if not self.text:
            # We assume html is present
            self.convert_html_to_text()

        # Load the Swedish language model
        nlp = spacy.load("sv_core_news_sm")
        # add pipeline (declared through entry_points in setup.py)
        nlp.add_pipe("entityLinker", last=True)

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
                print(sent._.linkedEntities.pretty_print())
                entities_objects = sent._.linkedEntities
                entities = set([f"Q{entity.get_id()}" for entity in entities_objects])
                entities = self.fix_obvious_wrong_entities(entities)
                logger.info(f"Sentence text: {cleaned_sentence}, entities: {entities}, tokens: {token_count}")
                # if cleaned_sentence and entities:
                #     exit()
                filtered_sentences = [
                    Sentence(text=sent.text, token_count=token_count,entities=entities)
                    for sent in doc.sents
                    if sent.text.strip()
                ]
                self.sentences.extend(filtered_sentences)
            count += 1

        logger.info(f"Extracted {len(self.sentences)} sentences")

    def fix_obvious_wrong_entities(self, entities: set):
        for wrong_entity, correct_entity in self.custom_substitutions.items():
            entities = list(map(lambda s: s.replace(wrong_entity, correct_entity), entities))
        return entities

    @property
    def custom_substitutions(self):
        return {
            'Q19896337': 'Q34',
            'Q3143': 'Q46', #moon ->europa
            # Add more substitutions as needed
            # 'Q_ENTITY_X': 'Q_ENTITY_Y'
        }

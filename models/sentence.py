import logging
import string
import uuid
from typing import Any, List

from ftlangdetect import detect
from pydantic import BaseModel
from spacy.language import Doc
from spacy.tokens import Span

import config
from models.crud.insert import Insert
from models.crud.read import Read
from models.entities import Entities
from models.token import Token

logger = logging.getLogger(__name__)


class Sentence(BaseModel):
    sent: Span
    doc: Doc
    document: Any
    accepted_tokens: List[Token] = list()
    uuid: str = ""
    score: float = 0.0
    detected_language: str = ""

    class Config:
        arbitrary_types_allowed = True

    @property
    def text(self) -> str:
        return str(self.sent.text)

    @property
    def id(self) -> int:
        """ID of this rawtoken in the database"""
        read = Read()
        read.connect_and_setup()
        data = read.get_sentence_id(sentence=self)
        read.close_db()
        return data

    @property
    def score_id(self) -> int:
        """ID of this score in the database"""
        read = Read()
        read.connect_and_setup()
        data = read.get_score(sentence=self)
        read.close_db()
        return data

    @property
    def language_id(self) -> int:
        """ID of this language in the database"""
        read = Read()
        read.connect_and_setup()
        data = read.get_language(sentence=self)
        read.close_db()
        return data

    @property
    def is_suitable_sentence(self) -> bool:
        # Check if the sentence has more than 5 words after cleaning
        if self.number_of_words_in_clean_sentence > 5:
            return True
        else:
            return False

    @property
    def cleaned_sentence(self) -> str:
        # Remove newlines, digits and a selection of characters
        sentence = self.text
        sentence = (
            sentence.replace("\t", " ")
            .replace(":", "")
            .replace("(", "")
            .replace(")", "")
            .replace("-", "")
            .replace("–", "")
            .replace("/", "")
        )
        sentence = "".join(char for char in sentence if char not in string.punctuation)
        # Split the sentence into words and remove words containing numbers
        words = [
            word
            for word in sentence.split()
            if not any(char.isdigit() for char in word)
        ]
        sentence = " ".join(words)
        # common_one_word_sentences = ['metod', 'slutsats', 'tabell',
        #                              'problem', 'problemformulering', 'bilaga']
        # for word in common_one_word_sentences:
        #     if sentence.lower() == word:
        #         sentence = ""
        return sentence

    @property
    def number_of_words_in_clean_sentence(self) -> int:
        return len(self.cleaned_sentence.split())

    @property
    def has_content_after_cleaning(self) -> bool:
        return bool(self.cleaned_sentence)

    @property
    def has_acceptable_score(self) -> bool:
        return self.score >= 0.4

    def analyze_and_insert(self):
        # We insert and store valuable tokens even if the
        # sentence is not deemed suitable for our purposes
        # Language detection is needed to find out
        # it if already exists in the database
        if not self.has_content_after_cleaning:
            logger.debug("Skipped empty sentence")
        else:
            # We avoid inserting garbage tokens from sentences
            # which are too short
            if self.number_of_words_in_clean_sentence == 1:
                logger.debug(
                    "Skipping token from one-word sentence\n"
                    f"cleaned sentence: {self.cleaned_sentence}"
                )
            else:
                # Found more than one word
                self.detect_language()
                self.insert_score()
                self.iterate_tokens()
                # We don't trust scores below this threshold
                # They are often occur when only one or two tokens
                # are left after cleaning
                # Also don't add sentences with these unlikely languages as
                # they are just hallucinations by the model
                if (
                    self.is_suitable_sentence
                    and self.detected_language in config.accepted_languages
                    and self.has_acceptable_score
                ):
                    sentence_id = self.id
                    if not sentence_id:
                        # self.print_ner_result()
                        self.generate_uuid()
                        self.insert_sentence_and_entities_and_link()
                    else:
                        logger.debug("Skipping sentence we already have analyzed")
                else:
                    # we could insert these hallucinations in new table discarded,
                    # but they are not worth much
                    logger.debug(
                        "Skipping unsuitable sentence which is too short, "
                        "with language detection score below 0.4 "
                        "or a language code we accept:\n"
                        f"lang: {self.detected_language}\n"
                        f"score: {self.score}\n"
                        f"cleaned word count: {self.number_of_words_in_clean_sentence}\n"
                        f"cleaned sentence: {self.cleaned_sentence}"
                    )

    def insert_score(self):
        read = Read()
        read.connect_and_setup()
        score = read.get_score(sentence=self)
        read.close_db()
        if not score:
            insert = Insert()
            insert.connect_and_setup()
            insert.insert_score(sentence=self)
            insert.close_db()

    def iterate_tokens(self):
        for token_ in self.sent:
            token = Token(token=token_, sentence=self)
            token.analyze_and_insert()
            if token.is_accepted_token:
                self.accepted_tokens.append(token)
            else:
                logger.debug(
                    f"Discarded token: '{token.rawtoken}@{self.detected_language}'"
                )

    # def clean_and_print_sentence(self):
    #     logger.info(
    #         f"Sentence text: {self.cleaned_sentence}, "
    #         f"Token count: {self.cleaned_sentence.split()}"
    #     )

    def generate_uuid(self) -> None:
        # Generate UUIDs for each sentence and add them to a new 'uuid' column
        self.uuid = str(uuid.uuid4())

    def detect_language(self) -> None:
        """Detect language using fasttext"""
        # This returns a dict like so: {'lang': 'tr', 'score': 0.9982126951217651}
        # We clean the sentence before language detection to avoid garbage results
        cleaned_sentence = self.cleaned_sentence
        # TODO implement swedish lexeme based detection if < 4 words
        if cleaned_sentence:
            result = detect(text=cleaned_sentence, low_memory=False)
            # if result["score"] < 0.4:
            #     # This can be caused by the cleaning,
            #     # so try alternative cleaning where newlines
            #     # are replaced with spaces
            # We round the score because the extra decimals do not add anything of value
            self.score = round(result["score"], 2)
            self.detected_language = result["lang"]
        else:
            logger.warning(f"No content after cleaning, skipping language detection")

    def print_ner_result(self) -> None:
        count = 0
        for entity in self.doc.ents:
            if entity.start >= self.sent.start and entity.end <= self.sent.end:
                if count == 0:
                    print("NER result:")
                print(f"{entity.text} -> {entity.label_}")
                count += 1
        # sleep(1)

    def insert_sentence_and_entities_and_link(self):
        logger.debug("Inserting sentence, entities and linking them all")
        insert = Insert()
        insert.connect_and_setup()
        sentence_id = insert.insert_sentence(sentence=self)
        insert.link_sentence_to_rawtokens(sentence=self)
        insert.close_db()
        entities = Entities(sentence_id=sentence_id, sentence=self)
        entities.extract_and_insert()

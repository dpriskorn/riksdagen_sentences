import logging
import re
import string
import uuid
from typing import Any, List, Dict
from ftlangdetect import detect

from pydantic import BaseModel

from models.token import Token

logger = logging.getLogger(__name__)


class Sentence(BaseModel):
    sent: Any
    database_handler: Any
    document: Any
    tokens: List[Token] = list()
    uuid: str = ""
    score: float = 0.0
    detected_language: str = ""

    class Config:
        arbitrary_types_allowed = True

    @property
    def sentence(self) -> str:
        return str(self.sent)

    @property
    def id(self) -> int:
        """ID of this rawtoken in the database"""
        return self.database_handler.get_sentence_id(sentence=self)

    @property
    def score_id(self) -> int:
        """ID of this score in the database"""
        return self.database_handler.get_score(sentence=self)

    @property
    def language_id(self) -> int:
        """ID of this language in the database"""
        return self.database_handler.get_language(sentence=self)

    @property
    def is_suitable_sentence(self) -> bool:
        # Removing punctuation
        sentence_without_punctuation = "".join(
            char for char in str(self.sent) if char not in string.punctuation
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

    @property
    def cleaned_sentence(self) -> str:
        # Remove newlines, digits and a selection of characters
        return re.sub(
            r"\d+",
            "",
            self.sent.text.replace("\n", "")
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

    def start(self):
        self.clean_and_print_sentence()
        # We insert and store valuable tokens even if the
        # sentence is not deemed suitable for our purposes
        self.detect_language()
        self.database_handler.insert_score(sentence=self)
        self.iterate_tokens()
        if self.is_suitable_sentence:
            self.generate_uuid()
            self.database_handler.insert_sentence(sentence=self)
            # self.database_handler.link_sentence_to_rawtokens(sentence=self)
            # todo iterate self.tokens and link between this sentence and their id

    def iterate_tokens(self):
        for token_ in self.sent:
            token = Token(token=token_, sentence=self)
            token.analyze_and_insert()
            self.tokens.append(token)

    def clean_and_print_sentence(self):
        logger.info(
            f"Sentence text: {self.cleaned_sentence}, "
            f"Token count: {self.cleaned_sentence.split()}"
        )

    def generate_uuid(self) -> None:
        # Generate UUIDs for each sentence and add them to a new 'uuid' column
        self.uuid = str(uuid.uuid4())

    def detect_language(self) -> None:
        """Detect language using fasttext"""
        # This returns a dict like so: {'lang': 'tr', 'score': 0.9982126951217651}
        result = detect(text=self.sentence.replace("\n", ""), low_memory=False)
        # We round the score because the extra decimals do not add anything of value
        self.score = round(result["score"], 2)
        self.detected_language = result["lang"]

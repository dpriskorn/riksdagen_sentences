import logging
import re
import string
from typing import Any, List

from pydantic import BaseModel

from models.token import Token

logger = logging.getLogger(__name__)


class Sentence(BaseModel):
    sent: Any
    database_handler: Any
    tokens: List[Token] = list()

    class Config:
        arbitrary_types_allowed = True

    def start(self):
        self.clean_and_print_sentence()
        # We insert and store valuable tokens even if the
        # sentence is not deemed suitable for our purposes
        self.iterate_tokens()
        # if self.is_suitable_sentence:
        #     self.database_handler.insert_sentence(sentence=self)
        # todo iterate self.tokens and link between this sentence and their id

    @property
    def id(self) -> int:
        """ID of this rawtoken in the database"""
        return self.database_handler.get_sentence_id(sentence=self)

    def iterate_tokens(self):
        for token_ in self.sent:
            token = Token(token=token_, database_handler=self.database_handler)
            token.start()
            self.tokens.append(token)

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

    def clean_and_print_sentence(self):
        logger.info(
            f"Sentence text: {self.cleaned_sentence}, "
            f"Token count: {self.cleaned_sentence.split()}"
        )

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

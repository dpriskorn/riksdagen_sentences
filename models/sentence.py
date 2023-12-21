import logging
from typing import Any
import re
from pydantic import BaseModel

from models.database_handler import DatabaseHandler
from models.token import Token

logger = logging.getLogger(__name__)


class Sentence(BaseModel):
    sent: Any
    database_handler: DatabaseHandler

    class Config:
        arbitrary_types_allowed = True

    def start(self):
        self.clean_and_print_sentence()
        self.iterate_tokens()

    def iterate_tokens(self):
        for token in self.sent:
            mytoken = Token(token=token, database_handler=self.database_handler)
            mytoken.start()

    def clean_and_print_sentence(self, sent):
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

        logger.info(
            f"Sentence text: {cleaned_sentence}, "
            f"Token count: {cleaned_sentence.split()}"
        )

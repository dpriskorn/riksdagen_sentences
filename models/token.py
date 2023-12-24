import logging
import re
from typing import Any

from pydantic import BaseModel

import config
from models.crud.insert import Insert
from models.crud.read import Read

logger = logging.getLogger(__name__)


class Token(BaseModel):
    token: Any  # token from spaCy
    sentence: Any

    def analyze_and_insert(self):
        if self.is_accepted_token:
            # logger.debug(spacy.explain(self.pos))
            logger.debug(f"rawtoken: '{self.rawtoken}'")
            logger.debug(f"normtoken: '{self.normalized_token}'")
            read = Read()
            read.connect_and_setup()
            rawtoken_id = read.get_rawtoken_id(token=self)
            insert = Insert()
            insert.connect_and_setup()
            if not rawtoken_id:
                insert.insert_rawtoken(token=self)
            normtoken_id = read.get_normtoken_id(token=self)
            read.close_db()
            if not normtoken_id:
                insert.insert_normtoken(token=self)
            insert.link_normtoken_to_rawtoken(token=self)
            insert.close_db()
        else:
            logger.debug(f"discarded: text: '{self.rawtoken}', pos: {self.pos}")

    @property
    def id(self) -> int:
        """ID of this rawtoken in the database"""
        read = Read()
        read.connect_and_setup()
        data = read.get_rawtoken_id(token=self)
        read.close_db()
        return data

    @property
    def normtoken_id(self) -> int:
        """ID of the corresponding normtoken for this token in the database"""
        read = Read()
        read.connect_and_setup()
        data = read.get_normtoken_id(token=self)
        read.close_db()
        return data

    @property
    def pos_id(self) -> int:
        """ID of the postag of this token in the database"""
        read = Read()
        read.connect_and_setup()
        data = read.get_lexical_category_id(token=self)
        read.close_db()
        return data

    @property
    def pos(self) -> str:
        return self.token.pos_

    @property
    def rawtoken(self) -> str:
        return str(self.token.text)

    @property
    def normalized_token(self) -> str:
        """We don't clean away punctuation and other chars here"""
        return str(self.token.text).strip().lower()

    @property
    def is_accepted_token(self) -> bool:
        """We accept a token which is has no
        numeric characters and is not a symbol and
        not punctuation and has a detected language we accept"""
        unaccepted_postags = ["SPACE", "PUNCT", "SYM", "X"]
        unaccepted_chars = ["¶", "¤", "¥", "~", "$", "€", "|"]
        if self.cleaned_token:
            has_numeric = any(char.isnumeric() for char in self.rawtoken)
            has_unacceptable_char = any(char in unaccepted_chars for char in self.rawtoken)
            if (
                self.token.pos_ not in unaccepted_postags
                and not has_numeric
                and not has_unacceptable_char
                and self.sentence.detected_language in config.accepted_languages
            ):
                return True
        return False

    @property
    def cleaned_token(self) -> str:
        # Remove newlines, digits and a selection of characters
        return re.sub(
            r"\d+",
            "",
            (self.rawtoken
            .replace("\r", "")
             .replace(":", "")
            .replace(",", "")
            .replace(".", "")
            .replace("(", "")
            .replace(")", "")
            .replace("-", "")
            .replace("–", "")
            .replace("/", "")
            .strip()),
        )

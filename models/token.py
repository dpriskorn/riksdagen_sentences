import logging
from typing import Any
import re
import spacy
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class Token(BaseModel):
    token: Any  # token from spaCy
    sentence: Any

    def analyze_and_insert(self):
        if self.is_accepted_token():
            print(spacy.explain(self.pos))
            print(f"rawtoken: '{self.rawtoken}'")
            print(f"normtoken: '{self.normalized_token()}'")
            self.sentence.database_handler.insert_rawtoken(token=self)
        else:
            print(f"discarded: text: {self.rawtoken}, pos: {self.pos}")

    @property
    def id(self) -> int:
        """ID of this rawtoken in the database"""
        return self.database_handler.get_rawtoken_id(token=self)

    @property
    def pos_id(self) -> int:
        """ID of the postag of this token in the database"""
        return self.sentence.database_handler.get_lexical_category_id(token=self)

    @property
    def pos(self) -> str:
        return self.token.pos_

    @property
    def rawtoken(self) -> str:
        return str(self.token.text)

    def normalized_token(self) -> str:
        return str(self.token.text).strip().lower()

    def is_accepted_token(self) -> bool:
        """We accept a token which is has no
        numeric characters and is not a symbol and
        not punctuation"""
        unaccepted_postags = ["SPACE", "PUNCT", "SYM", "X"]
        if self.cleaned_token:
            has_numeric = any(char.isnumeric() for char in self.rawtoken)
            if self.token.pos_ not in unaccepted_postags and not has_numeric:
                return True
        return False

    @property
    def cleaned_token(self) -> str:
        # Remove newlines, digits and a selection of characters
        return re.sub(
            r"\d+",
            "",
            self.rawtoken.replace(":", "")
            .replace(",", "")
            .replace(".", "")
            .replace("(", "")
            .replace(")", "")
            .replace("-", "")
            .replace("–", "")
            .replace("/", "")
            .strip(),
        )

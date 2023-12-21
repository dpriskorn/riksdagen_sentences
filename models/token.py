from typing import Any

import spacy
from pydantic import BaseModel

from models.database_handler import DatabaseHandler


class Token(BaseModel):
    token: Any
    database_handler: DatabaseHandler

    def start(self):
        if self.is_accepted_token():
            print(spacy.explain(self.pos))
            print(f"rawtoken: '{self.rawtoken}'")
            print(f"normtoken: '{self.normalized_token()}'")
            self.database_handler.insert_rawtoken(token=self)
            exit()
        else:
            print(f"discarded: text: {self.rawtoken}, pos: {self.pos}")

    @property
    def id(self) -> int:
        """ID of this rawtoken in the database"""
        return self.database_handler.get_rawtoken_id(token=self)

    @property
    def pos_id(self) -> int:
        """ID of the postag of this token in the database"""
        return self.database_handler.get_lexical_category_id(token=self)

    @property
    def pos(self) -> str:
        return self.token.pos_

    @property
    def rawtoken(self) -> str:
        return self.token.text

    def normalized_token(self) -> str:
        return str(self.token.text).strip().lower()

    def is_accepted_token(self) -> bool:
        """We accept a token which is not only
        numeric and not symbol and not punctuation"""
        unaccepted_postags = ["SPACE", "PUNCT", "SYM", "X"]
        numeric = str(self.token.text).strip().isnumeric()
        if self.token.pos_ not in unaccepted_postags and not numeric:
            return True
        else:
            return False

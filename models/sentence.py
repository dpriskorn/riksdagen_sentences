from typing import List, Set

from pydantic import BaseModel


class Sentence(BaseModel):
    text: str
    token_count: int
    entities: Set[str]

    def __str__(self):
        return self.text

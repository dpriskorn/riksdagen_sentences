from pydantic import BaseModel


class Sentence(BaseModel):
    text: str
    token_count: int

    def __str__(self):
        return self.text

from pydantic import BaseModel


class SentenceAttributes(BaseModel):
    text: str
    score: float


class SentenceResult(BaseModel):
    """This follows the JSON API 1.1 spec"""

    attributes: SentenceAttributes
    type: str = "sentence"
    id: str  # uuid

    def dump_model(self):
        return self.model_dump()
from pydantic import BaseModel

from models.crud.insert import Insert
from models.crud.read import Read
from models.exceptions import MissingInformationError


class Entity(BaseModel):
    """Hashable entity that can be deduplicated"""

    label: str
    ner_label: str

    def __hash__(self):
        return hash((self.label, self.ner_label))

    def __eq__(self, other):
        return self.label == other.label and self.ner_label == other.ner_label

    @property
    def ner_label_id(self) -> int:
        read = Read()
        read.connect_and_setup()
        data = read.get_ner_label_id(entity=self)
        read.close_db()
        return data

    @property
    def entity_id(self):
        read = Read()
        read.connect_and_setup()
        data = read.get_entity_id(entity=self)
        read.close_db()
        return data

    def __insert(self) -> int:
        insert = Insert()
        insert.connect_and_setup()
        id_ = insert.insert_entity(entity=self)
        if not id_:
            raise MissingInformationError(
                "Did not get an entity id back from the database"
            )
        return id_

    def check_and_insert_if_missing(self):
        if not self.entity_id:
            self.__insert()

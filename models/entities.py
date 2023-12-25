import logging
from typing import List, Any, Set

from pydantic import BaseModel

from models.crud.insert import Insert
from models.entity import Entity

logger = logging.getLogger(__name__)


class Entities(BaseModel):
    sentence_id: int
    entities: Set[Entity] = set()
    sentence: Any

    def extract_and_insert(self):
        self.__extract()
        self.__insert()

    def __extract(self) -> None:
        for ent in self.sentence.doc.ents:
            if ent.start >= self.sentence.sent.start and ent.end <= self.sentence.sent.end:
                self.entities.add(Entity(label=ent.text, ner_label=ent.label_))

    def __insert(self):
        logger.debug("Inserting entities")
        if self.entities:
            # We use a list here to save some queries to the database
            entity_ids = list()
            for entity in self.entities:
                entity.check_and_insert_if_missing()
                entity_ids.append(entity.entity_id)
            # todo move this to Entity
            insert = Insert()
            insert.connect_and_setup()
            for entity_id in entity_ids:
                insert.link_sentence_to_entity(entity_id=entity_id, sentence_id=self.sentence_id)
            insert.close_db()

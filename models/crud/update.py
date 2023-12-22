import logging
from typing import Any

from models.crud.database_handler import Mariadb

logger = logging.getLogger(__name__)


class Update(Mariadb):
    def update_document_as_processed(self, document: Any):
        query = """UPDATE document
            SET processed = True
            WHERE id = %s;
        """
        params = (document.id,)
        self.cursor.execute(query, params)
        self.commit_to_database()
        logger.info("updated document as processed")

import logging
from typing import Dict, Any

import pymysql
from pydantic import BaseModel
from pymysql.connections import Connection
from pymysql.cursors import Cursor

logger = logging.getLogger(__name__)


class Mariadb(BaseModel):
    lexical_categories: Dict[Any, Any] = dict()
    languages: Dict[Any, Any] = dict()
    language_config_path: str = "config/languages.yml"
    lexical_categories_config_path: str = "config/lexical_categories.yml"
    connection: Connection = None
    # cursor: Cursor = None
    # row_cursor: Cursor = None
    cursor: Cursor = None

    class Config:
        arbitrary_types_allowed = True

    @staticmethod
    def item_int(qid) -> int:
        return int(qid[1:])

    def connect_and_setup(self):
        pass

    def connect_to_mariadb(self):
        """Connect to a local database"""
        # Connection parameters
        host = "localhost"
        user = "riksdagen"
        password = "password"
        database = "riksdagen"

        # Connect to the database
        try:
            self.connection = pymysql.connect(
                host=host, user=user, passwd=password, db=database
            )
            logger.info("succesfully connected to mariadb")

        except pymysql.Error as e:
            print("Error: %s" % e)

    def initialize_mariadb_cursor(self) -> None:
        self.cursor = self.connection.cursor()

    def commit_to_database(self) -> None:
        self.connection.commit()

    def close_db(self) -> None:
        self.connection.close()

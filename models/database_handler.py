import logging
import sqlite3
from sqlite3 import DatabaseError
from typing import Dict, Any

import pymysql
from pymysql.cursors import Cursor
from pymysql.connections import Connection
import yaml
from pydantic import BaseModel

from models.exceptions import PostagError
from models.token import Token

logger = logging.getLogger(__name__)


class DatabaseHandler(BaseModel):
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
        self.load_languages_from_yaml()
        self.load_lexical_categories_from_yaml()
        self.connect_to_mariadb()
        self.initialize_mariadb_cursor()
        self.create_tables()
        self.create_indexes()
        self.setup_languages()
        self.setup_lexical_categories()
        self.commit_to_database()

    def connect_to_sqlite_db(self) -> None:
        db_file = "database.db"
        # Connect to the database
        self.connection = sqlite3.connect(db_file)

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

    def create_tables(self):
        logger.info("Creating tables")
        # Note: we get weird errors if the foreign key columns are not the exakt same type
        sql_commands = [
            """CREATE TABLE IF NOT EXISTS lexical_category (
                id SMALLINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                qid SMALLINT UNSIGNED NOT NULL,
                postag TEXT UNIQUE NOT NULL
            );""",
            """CREATE TABLE IF NOT EXISTS language (
                id SMALLINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                name_en TEXT NOT NULL UNIQUE,
                iso_code TEXT NOT NULL UNIQUE,
                qid TEXT NOT NULL UNIQUE
            );""",
            """CREATE TABLE IF NOT EXISTS provider (
                id SMALLINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                title TEXT NOT NULL,
                qid INT UNSIGNED NOT NULL
            );""",
            """CREATE TABLE IF NOT EXISTS collection (
                id SMALLINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                title TEXT NOT NULL,
                qid INT UNSIGNED NOT NULL,
                provider SMALLINT UNSIGNED NOT NULL,
                FOREIGN KEY (provider) REFERENCES provider(id)
            );""",
            """CREATE TABLE IF NOT EXISTS dataset (
                id SMALLINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                title TEXT NOT NULL,
                qid INT UNSIGNED NOT NULL UNIQUE,
                collection SMALLINT UNSIGNED NOT NULL,
                FOREIGN KEY (collection) REFERENCES collection(id)
            );""",
            """CREATE TABLE IF NOT EXISTS document (
                id SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                dataset SMALLINT UNSIGNED NOT NULL,
                external_id VARCHAR(255) NOT NULL,
                FOREIGN KEY (dataset) REFERENCES dataset(id),
                UNIQUE(dataset, external_id)
            );
            """,
            """CREATE TABLE IF NOT EXISTS score (
                id SMALLINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                value FLOAT NOT NULL UNIQUE
            );""",
            """CREATE TABLE IF NOT EXISTS sentence (
                id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                text TEXT NOT NULL,
                uuid VARCHAR(255) NOT NULL UNIQUE,
                document SMALLINT UNSIGNED NOT NULL,
                score SMALLINT UNSIGNED NOT NULL,
                language SMALLINT UNSIGNED NOT NULL,
                # UNIQUE (text, document),
                FOREIGN KEY (document) REFERENCES document(id),
                FOREIGN KEY(language) REFERENCES language(id),
                FOREIGN KEY(score) REFERENCES score(id)
            );""",
            """CREATE TABLE IF NOT EXISTS lexeme_form (
                id SMALLINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                lexeme INT UNSIGNED NOT NULL,
                form SMALLINT UNSIGNED NOT NULL,
                UNIQUE (lexeme, form)
            );""",
            """CREATE TABLE IF NOT EXISTS rawtoken (
                id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                lexical_category SMALLINT UNSIGNED NOT NULL,
                text VARCHAR(255) NOT NULL,
                score SMALLINT UNSIGNED NOT NULL,
                language SMALLINT UNSIGNED NOT NULL,
                FOREIGN KEY(lexical_category) REFERENCES lexical_category(id),
                FOREIGN KEY(language) REFERENCES language(id),
                FOREIGN KEY(score) REFERENCES score(id),
                UNIQUE(text, lexical_category, language)
            );""",
            """CREATE TABLE IF NOT EXISTS normtoken (
                id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                text VARCHAR(255) NOT NULL UNIQUE
            );""",
            """CREATE TABLE IF NOT EXISTS rawtoken_normtoken_linking (
                rawtoken INT UNSIGNED NOT NULL,
                normtoken INT UNSIGNED NOT NULL,
                PRIMARY KEY (rawtoken, normtoken),
                FOREIGN KEY (rawtoken) REFERENCES rawtoken(id),
                FOREIGN KEY (normtoken) REFERENCES normtoken(id)
            );""",
            """CREATE TABLE IF NOT EXISTS rawtoken_sentence_linking (
                sentence INT UNSIGNED NOT NULL,
                rawtoken INT UNSIGNED NOT NULL,
                PRIMARY KEY (sentence, rawtoken),
                FOREIGN KEY (sentence) REFERENCES sentence(id),
                FOREIGN KEY (rawtoken) REFERENCES rawtoken(id)
            );""",
            """CREATE TABLE IF NOT EXISTS rawtoken_lexeme_form_linking (
                rawtoken INT UNSIGNED NOT NULL,
                lexeme_form SMALLINT UNSIGNED NOT NULL,
                PRIMARY KEY (rawtoken, lexeme_form),
                FOREIGN KEY (rawtoken) REFERENCES rawtoken(id),
                FOREIGN KEY (lexeme_form) REFERENCES lexeme_form(id)
            );""",
        ]
        for query in sql_commands:
            logger.debug(f"execuring: {query}")
            self.cursor.execute(query)

    def create_indexes(self):
        """These indexes enable us to fast lookup of sentences
        in a given language, document or with a given UUID"""
        logger.info("Creating indexes")
        sql_index_queries = [
            "CREATE INDEX IF NOT EXISTS idx_score_value ON score(value);",
            "CREATE INDEX IF NOT EXISTS idx_language_iso_code ON language(iso_code);",
            "CREATE INDEX IF NOT EXISTS idx_postag ON lexical_category(postag);",
            "CREATE INDEX IF NOT EXISTS idx_sentence_uuid ON sentence(uuid);",
            "CREATE INDEX IF NOT EXISTS idx_sentence_document_id ON sentence(document);",
            """CREATE INDEX IF NOT EXISTS idx_rawtoken_text ON rawtoken(text);""",
            """CREATE INDEX IF NOT EXISTS idx_normtoken_text ON normtoken(text);""",
        ]
        for query in sql_index_queries:
            self.cursor.execute(query)

    def commit_to_database(self) -> None:
        self.connection.commit()

    def close_db(self) -> None:
        self.connection.close()

    def load_languages_from_yaml(self):
        # Load YAML into a dictionary
        with open(self.language_config_path, "r") as file:
            # Read YAML content from the file
            self.languages = yaml.safe_load(file)

    def load_lexical_categories_from_yaml(self):
        # Load YAML into a dictionary
        with open(self.lexical_categories_config_path, "r") as file:
            # Read YAML content from the file
            self.lexical_categories = yaml.safe_load(file)

    def setup_languages(self):
        print("Inserting languages from YAML")
        # Construct the SQL INSERT query
        query = """
                INSERT IGNORE INTO language (name_en, iso_code, qid)
                VALUES (%s, %s, %s)
                """

        # Iterate through each language and insert its data
        for lang_code, lang_data in self.languages["development"].items():
            language_name_en = lang_data["language_name_en"]
            iso_code = lang_code
            qid = lang_data["language_qid"]

            # Execute the query with the extracted values for each language
            self.cursor.execute(query, (language_name_en, iso_code, qid))
        self.commit_to_database()

    def setup_lexical_categories(self):
        print("Inserting lexical categories from YAML")
        for postag, qid in self.lexical_categories.items():
            self.cursor.execute(
                "INSERT IGNORE INTO lexical_category (qid, postag) VALUES (%s, %s)",
                (qid, postag),
            )
        self.commit_to_database()
        # print("Categories inserted successfully!")

    def insert_dataset_in_database(self, dataset_handler: Any):
        print("Setting up dataset entry")
        item_int = self.item_int(dataset_handler.dataset_wikidata_qid)
        query = """
        INSERT INTO dataset (title, qid, collection)
        VALUES (%s, %s, %s)
        """
        title = dataset_handler.riksdagen_dataset_title
        done_query = self.cursor.mogrify(
            query, (title, item_int, dataset_handler.collection_id)
        )
        print(done_query)
        self.cursor.execute(query, (title, item_int, dataset_handler.collection_id))
        self.commit_to_database()

    def get_dataset_id(self, dataset_handler: Any):
        item_int = self.item_int(dataset_handler.dataset_wikidata_qid)
        query = """SELECT id
            FROM dataset
            WHERE qid = %s;
        """
        done_query = self.cursor.mogrify(query, (item_int,))
        print(done_query)
        self.cursor.execute(query, (item_int,))
        result = self.cursor.fetchone()
        print(result)
        if result:
            dataset_id = [0]
            print(f"Got dataset id: {dataset_id}")
            return dataset_id

    def find_rawtoken_with_certain_lexical_category(
        self, rawtoken: str, lexical_category: str
    ):
        lexcat_item_int = self.item_int(lexical_category)
        query = """
            SELECT *
            FROM rawtoken
            JOIN lexical_category ON rawtoken.lexical_category_id = lexical_category.id
            WHERE rawtoken.text = ?
            AND rawtoken.lexical_category_id = ?
            """
        self.row_cursor.execute(query, (rawtoken, lexcat_item_int))
        results = self.row_cursor.fetchall()
        return results

    def find_sentences_for_rawtoken_id(self, rawtoken_id: int):
        query = """
        SELECT * FROM sentence
        JOIN rawtoken_sentence_linking ON sentence.id = rawtoken_sentence_linking.sentence_id
        WHERE rawtoken_sentence_linking.rawtoken_id = ?
        """
        self.row_cursor.execute(query, (rawtoken_id,))
        results = self.row_cursor.fetchall()
        return results

    def add_document_to_database(self, document: Any):
        print("Adding document to database")
        # Assuming 'documents' is the name of the table where documents are stored
        query = "INSERT IGNORE INTO document (external_id, dataset) VALUES (%s, %s)"
        values = (document.external_id, document.dataset_id)
        try:
            self.row_cursor.execute(query, values)
            self.connection.commit()
            print("Document added to the database.")
        except sqlite3.Error as e:
            raise DatabaseError(f"Error adding document to the database: {e}")
        finally:
            self.commit_to_database()

    def get_lexical_category_id(self, token: Token) -> int:
        query = """SELECT id
            FROM lexical_category
            WHERE postag = ?;
        """
        self.cursor.execute(query, (token.pos,))
        result = self.cursor.fetchone()
        if not result:
            raise PostagError(f"postag {token.pos} not found in database")
        else:
            rowid = result[0]
        print(f"Got lexical category rowid: {rowid}")
        return rowid

    def insert_rawtoken(self, token: Token):
        query = """
        INSERT IGNORE INTO rawtoken 
        (lexical_category, text, language, score)
        VALUES (%s, %s, %s, %s);
        """
        params = (
            token.pos_id,
            token.rawtoken,
            token.sentence.language_id,
            token.sentence.score_id,
        )
        self.cursor.execute(query, params)
        self.commit_to_database()
        logger.info("rawtoken inserted")

    def get_rawtoken_id(self, token: Token):
        query = """SELECT id
            FROM rawtoken
            WHERE text = ? and lexical_category = ?;
        """
        self.cursor.execute(query, (token.rawtoken, token.pos_id))
        rowid = self.cursor.fetchone()[0]
        logger.info(f"Got rawtoken id: {rowid}")
        return rowid

    def insert_sentence(self, sentence: Any):
        query = """
        INSERT IGNORE INTO sentence (text, uuid, document, language, score)
        VALUES (%s, %s, %s, %s, %s);
        """
        params = (
            sentence.sentence,
            sentence.uuid,
            sentence.document.id,
            sentence.language_id,
            sentence.score_id,
        )
        self.cursor.execute(query, params)
        self.commit_to_database()
        logger.info("sentence inserted")

    def insert_score(self, sentence: Any):
        query = """
        INSERT IGNORE INTO score (value)
        VALUES (%s)
        """
        params = (sentence.score,)
        self.cursor.execute(query, params)
        self.commit_to_database()
        logger.info("score inserted")

    def get_score(self, sentence: Any) -> int:
        query = """
            SELECT id
            FROM score
            WHERE value = ?
        """
        params = (sentence.score,)
        self.cursor.execute(query, params)
        score_id = self.cursor.fetchone()[0]
        logger.info(f"Got score id: {score_id}")
        return score_id

    def get_language(self, sentence: Any) -> int:
        query = """
            SELECT id
            FROM language
            WHERE iso_code = ?
        """
        params = (sentence.detected_language.lower(),)
        self.cursor.execute(query, params)
        rowid = self.cursor.fetchone()[0]
        logger.info(f"Got language id: {rowid}")
        return rowid

    def get_document_id(self, document: Any) -> int:
        query = """
            SELECT id
            FROM document
            WHERE external_id = ?
        """
        params = (document.external_id,)
        self.cursor.execute(query, params)
        rowid = self.cursor.fetchone()[0]
        logger.info(f"Got document id: {rowid}")
        return rowid

    def link_sentence_to_rawtokens(self, sentence: Any):
        for token in sentence.tokens:
            query = """
            INSERT IGNORE INTO rawtoken_sentence_linking (sentence, rawtoken)
            VALUES (%s, %s)
            """
            params = (sentence.id, token.id)
            self.cursor.execute(query, params)
            self.commit_to_database()
        logger.info("rawtoken <-> sentence links inserted")

    def get_sentence_id(self, sentence: Any) -> int:
        query = """
            SELECT id
            FROM sentence
            WHERE text = ? and document = ? and language = ?
        """
        params = (
            sentence.sentence,
            sentence.document.id,
            sentence.language_id,
        )
        self.cursor.execute(query, params)
        rowid = self.cursor.fetchone()[0]
        logger.info(f"Got sentence id: {rowid}")
        return rowid

    def insert_normtoken(self, token: Token):
        query = """
        INSERT IGNORE INTO normtoken 
        (text)
        VALUES (%s);
        """
        params = (token.normalized_token,)
        self.cursor.execute(query, params)
        self.commit_to_database()
        logger.info("normtoken inserted")

    def get_normtoken_id(self, token: Token):
        query = """SELECT id
            FROM normtoken
            WHERE text = ? 
        """
        self.cursor.execute(query, (token.normalized_token,))
        rowid = self.cursor.fetchone()[0]
        logger.info(f"Got normtoken id: {rowid}")
        return rowid

    def link_normtoken_to_rawtoken(self, token: Any):
        query = """
        INSERT IGNORE INTO rawtoken_normtoken_linking (normtoken, rawtoken)
        VALUES (%s, %s)
        """
        params = (token.normtoken_id, token.id)
        self.cursor.execute(query, params)
        self.commit_to_database()
        logger.info("rawtoken <-> normtoken link inserted")

    def link_lexeme_form_to_rawtoken(self, token: Any):
        # todo can we do this automatically? no?
        raise NotImplementedError()
        # query = """
        # INSERT IGNORE INTO rawtoken_lexeme_form_linking (rawtoken, lexeme, form)
        # VALUES (%s, %s, %s)
        # """
        # params = (
        #     token.normtoken_id,
        #     token.id
        # )
        # self.cursor.execute(query, params)
        # self.commit_to_database()
        # logger.info("rawtoken <-> normtoken link inserted")

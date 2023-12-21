import logging
import sqlite3
from sqlite3 import Cursor, DatabaseError
from typing import Dict, Any

import yaml
from pydantic import BaseModel

from models.token import Token

logger = logging.getLogger(__name__)


class PostagError(BaseException):
    pass


class DatabaseHandler(BaseModel):
    lexical_categories: Dict[Any, Any] = dict()
    languages: Dict[Any, Any] = dict()
    language_config_path: str = "config/languages.yml"
    lexical_categories_config_path: str = "config/lexical_categories.yml"
    connection: Any = None
    tuple_cursor: Cursor = None
    row_cursor: Cursor = None

    class Config:
        arbitrary_types_allowed = True

    @staticmethod
    def item_int(qid) -> int:
        return int(qid[1:])

    def connect_and_setup(self):
        self.load_languages_from_yaml()
        self.load_lexical_categories_from_yaml()
        self.connect_to_db()
        self.initialize_cursors()
        self.create_tables()
        self.create_indexes()
        self.setup_languages()
        self.setup_lexical_categories()
        self.commit_to_database()

    def connect_to_db(self) -> None:
        db_file = "database.db"
        # Connect to the database
        self.connection = sqlite3.connect(db_file)

    def initialize_cursors(self) -> None:
        # Create cursors to interact with the database
        self.row_cursor = self.connection.cursor()
        self.row_cursor.row_factory = sqlite3.Row
        self.tuple_cursor = self.connection.cursor()
        self.tuple_cursor.row_factory = None

    def create_tables(self):
        logger.info("Creating tables")
        sql_commands = [
            """CREATE TABLE IF NOT EXISTS lexical_category (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    qid INT UNIQUE,
                    postag TEXT UNIQUE NOT NULL
            );""",
            """CREATE TABLE IF NOT EXISTS language (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name_en TEXT NOT NULL UNIQUE,
                iso_code TEXT NOT NULL UNIQUE,
                qid TEXT NOT NULL UNIQUE
            );""",
            """CREATE TABLE IF NOT EXISTS provider (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                qid INT NOT NULL
            );""",
            """CREATE TABLE IF NOT EXISTS collection (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                qid INT NOT NULL,
                provider INT NOT NULL,
                FOREIGN KEY (provider) REFERENCES provider(id)
            );""",
            """CREATE TABLE IF NOT EXISTS dataset (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                qid INT NOT NULL UNIQUE,
                collection INT NOT NULL,
                FOREIGN KEY (collection) REFERENCES collection(id)
            );""",
            """CREATE TABLE IF NOT EXISTS document (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset INT NOT NULL,
                external_id TEXT NOT NULL,
                UNIQUE(dataset, external_id),
                FOREIGN KEY (dataset) REFERENCES dataset(id)
            );""",
            """CREATE TABLE IF NOT EXISTS sentence (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                text TEXT NOT NULL,
                uuid TEXT NOT NULL UNIQUE,
                document INT NOT NULL,
                language INT NOT NULL,
                score FLOAT NOT NULL,
                FOREIGN KEY (document) REFERENCES document(id),
                FOREIGN KEY (language) REFERENCES language(id)
            );""",
            """CREATE TABLE IF NOT EXISTS lexeme_form_id (
                lexeme_id INT NOT NULL,
                form_id INT NOT NULL,
                PRIMARY KEY (lexeme_id, form_id)
            );""",
            """CREATE TABLE IF NOT EXISTS rawtoken (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lexical_category_id INT NOT NULL,
                text TEXT NOT NULL,
                UNIQUE (text, lexical_category_id),
                FOREIGN KEY (lexical_category_id) REFERENCES lexical_category(id)
            );""",
            """CREATE TABLE IF NOT EXISTS normtoken (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                text TEXT NOT NULL UNIQUE
            );""",
            """CREATE TABLE IF NOT EXISTS raw_norm_linking (
                rawtoken_id INT NOT NULL,
                normtoken_id INT NOT NULL,
                PRIMARY KEY (rawtoken_id, normtoken_id),
                FOREIGN KEY (rawtoken_id) REFERENCES rawtoken(id),
                FOREIGN KEY (normtoken_id) REFERENCES normtoken(id)
            );""",
            """CREATE TABLE IF NOT EXISTS rawtoken_sentence_linking (
                sentence_id INT NOT NULL,
                rawtoken_id INT NOT NULL,
                PRIMARY KEY (sentence_id, rawtoken_id),
                FOREIGN KEY (sentence_id) REFERENCES sentence(id),
                FOREIGN KEY (rawtoken_id) REFERENCES rawtoken(id)
            );""",
            """CREATE TABLE IF NOT EXISTS rawtoken_lexeme_form_id_linking (
                rawtoken INT NOT NULL,
                lexeme_id INT NOT NULL,
                form_id INT NOT NULL,
                PRIMARY KEY (rawtoken, lexeme_id, form_id),
                FOREIGN KEY (rawtoken) REFERENCES rawtoken(id),
                FOREIGN KEY (lexeme_id) REFERENCES lexeme_form_id(lexeme_id),
                FOREIGN KEY (form_id) REFERENCES lexeme_form_id(form_id)
            );""",
        ]
        for query in sql_commands:
            self.tuple_cursor.execute(query)

    def create_indexes(self):
        """These indexes enable us to fast lookup of sentences
        in a given language, document or with a given UUID"""
        logger.info("Creating indexes")
        sql_index_queries = [
            "CREATE INDEX IF NOT EXISTS idx_language_id ON language(id);",
            "CREATE INDEX IF NOT EXISTS idx_postag ON lexical_category(postag);",
            "CREATE INDEX IF NOT EXISTS idx_provider_id ON provider(id);",
            "CREATE INDEX IF NOT EXISTS idx_collection_id ON collection(id);",
            "CREATE INDEX IF NOT EXISTS idx_dataset_id ON dataset(id);",
            "CREATE INDEX IF NOT EXISTS idx_document_id ON document(id);",
            "CREATE INDEX IF NOT EXISTS idx_sentence_id ON sentence(id);",
            "CREATE INDEX IF NOT EXISTS idx_sentence_uuid ON sentence(uuid);",
            "CREATE INDEX IF NOT EXISTS idx_sentence_document_id ON sentence(document);",
            "CREATE INDEX IF NOT EXISTS idx_sentence_language ON sentence(language);",
            """CREATE INDEX IF NOT EXISTS idx_rawtoken_text ON rawtoken(text);""",
            """CREATE INDEX IF NOT EXISTS idx_normtoken_text ON normtoken(text);""",
        ]
        for query in sql_index_queries:
            self.tuple_cursor.execute(query)

    def commit_to_database(self) -> None:
        # Don't forget to close the connection when done
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
                INSERT OR IGNORE INTO language (name_en, iso_code, qid)
                VALUES (?, ?, ?)
                """

        # Iterate through each language and insert its data
        for lang_code, lang_data in self.languages["development"].items():
            language_name_en = lang_data["language_name_en"]
            iso_code = lang_code
            qid = lang_data["language_qid"]

            # Execute the query with the extracted values for each language
            self.tuple_cursor.execute(query, (language_name_en, iso_code, qid))
        self.commit_to_database()

    def setup_lexical_categories(self):
        print("Inserting lexical categories from YAML")
        for postag, qid in self.lexical_categories.items():
            self.tuple_cursor.execute(
                "INSERT OR IGNORE INTO lexical_category (qid, postag) VALUES (?, ?)",
                (qid, postag),
            )
        self.commit_to_database()
        # print("Categories inserted successfully!")

    def insert_dataset_in_database(self, dataset_handler: Any):
        print("Setting up dataset entry")
        item_int = self.item_int(dataset_handler.dataset_wikidata_qid)
        query = """
        INSERT OR IGNORE INTO dataset (title, qid, collection)
        VALUES (?, ?, ?)
        """
        title = dataset_handler.riksdagen_dataset_title
        self.tuple_cursor.execute(
            query, (title, item_int, dataset_handler.collection_id)
        )
        self.commit_to_database()

    def get_dataset_id(self, dataset_handler: Any):
        item_int = self.item_int(dataset_handler.dataset_wikidata_qid)
        query = """SELECT id
            FROM dataset
            WHERE qid = ?;
        """
        self.tuple_cursor.execute(query, (item_int,))
        dataset_id = self.tuple_cursor.fetchone()[0]
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
        query = "INSERT OR IGNORE INTO document " "(external_id, dataset) VALUES (?, ?)"
        values = (document.id, document.dataset_id)
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
        self.tuple_cursor.execute(query, (token.pos,))
        rowid = self.tuple_cursor.fetchone()[0]
        if not rowid:
            raise PostagError()
        print(f"Got lexical category rowid: {rowid}")
        return rowid

    def insert_rawtoken(self, token: Token):
        query = """
        INSERT OR IGNORE INTO rawtoken (lexical_category_id, text)
        VALUES (?, ?);
        """
        params = (token.pos_id, token.rawtoken)
        self.tuple_cursor.execute(query, params)
        self.commit_to_database()
        logger.info("rawtoken inserted")

    def get_rawtoken_id(self, token: Token):
        query = """SELECT id
            FROM rawtoken
            WHERE text = ? and lexical_category_id = ?;
        """
        self.tuple_cursor.execute(query, (token.rawtoken,token.pos_id))
        dataset_id = self.tuple_cursor.fetchone()[0]
        print(f"Got dataset id: {dataset_id}")
        return dataset_id

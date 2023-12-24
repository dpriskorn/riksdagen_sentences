import logging

from models.crud.database_handler import Mariadb
from models.crud.insert import Insert

logger = logging.getLogger(__name__)


class Create(Mariadb):
    def connect_and_setup(self):
        self.connect_to_mariadb()
        self.initialize_mariadb_cursor()
        self.create_tables()
        self.create_indexes()
        self.insert_languages_and_lexical_categories_from_config()
        self.commit_to_database()

    @staticmethod
    def insert_languages_and_lexical_categories_from_config():
        insert = Insert()
        insert.connect_and_setup()
        # todo move this to own classes
        insert.load_languages_from_yaml()
        insert.load_lexical_categories_from_yaml()
        insert.insert_languages()
        insert.insert_lexical_categories()
        insert.close_db()

    def create_tables(self):
        logger.info("Creating tables")
        # Note: we get weird errors if the foreign key columns are not the exakt same type
        sql_commands = [
            """CREATE TABLE IF NOT EXISTS ner_label (
                id SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                label VARCHAR(30) NOT NULL UNIQUE,
                description VARCHAR(255) NOT NULL
            );
            """,
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
                workdirectory TEXT NOT NULL,
                qid INT UNSIGNED NOT NULL UNIQUE,
                collection SMALLINT UNSIGNED,
                FOREIGN KEY (collection) REFERENCES collection(id)
            );""",
            """CREATE TABLE IF NOT EXISTS document (
                id SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                dataset SMALLINT UNSIGNED NOT NULL,
                external_id VARCHAR(255) NOT NULL,
                processed BOOL DEFAULT FALSE,
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
                UNIQUE (text, document, language),
                FOREIGN KEY (document) REFERENCES document(id),
                FOREIGN KEY(language) REFERENCES language(id),
                FOREIGN KEY(score) REFERENCES score(id)
            );""",
            '''
            CREATE TABLE IF NOT EXISTS entity (
                id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                label VARCHAR(255) NOT NULL,
                ner_label SMALLINT UNSIGNED NOT NULL,
                UNIQUE (label, ner_label),
                FOREIGN KEY (ner_label) REFERENCES ner_label(id)
            )
            ''',
            """CREATE TABLE IF NOT EXISTS sentence_entity_linking (
                sentence INT UNSIGNED NOT NULL,
                entity INT UNSIGNED NOT NULL,
                PRIMARY KEY (sentence, entity),
                FOREIGN KEY (sentence) REFERENCES sentence(id),
                FOREIGN KEY (entity) REFERENCES entity(id)
            );
            """,
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

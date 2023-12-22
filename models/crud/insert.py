import logging
from typing import Any

import yaml

from models.crud.database_handler import Mariadb

logger = logging.getLogger(__name__)


class Insert(Mariadb):
    def connect_and_setup(self):
        self.connect_to_mariadb()
        self.initialize_mariadb_cursor()
        # todo move this to own classes
        self.load_languages_from_yaml()
        self.load_lexical_categories_from_yaml()
        self.insert_languages()
        self.insert_lexical_categories()

    def insert_languages(self):
        logger.info("Inserting languages from YAML")
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

    def insert_datasets_in_database(self, datasets):
        logger.info("Inserting datasets from YAML")
        query = """
                INSERT IGNORE INTO dataset (title, qid, workdirectory)
                VALUES (%s, %s, %s)
                """
        for title, data in datasets.raw_datasets.items():
            # todo support collection and lookup its id
            # collection = data.get("collection_qid")
            qid = data["qid"]
            workdirectory = data["workdirectory"]
            qid_int = self.item_int(qid)
            params = (title, qid, workdirectory)
            logger.debug(self.cursor.mogrify(query, params))
            self.cursor.execute(query, params)
        self.commit_to_database()

    def insert_lexical_categories(self):
        logger.info("Inserting lexical categories from YAML")
        for postag, qid in self.lexical_categories.items():
            self.cursor.execute(
                "INSERT IGNORE INTO lexical_category (qid, postag) VALUES (%s, %s)",
                (qid, postag),
            )
        self.commit_to_database()
        # print("Categories inserted successfully!")

    def insert_dataset_in_database(self, dataset_handler: Any):
        logger.info("Setting up dataset entry")
        item_int = self.item_int(dataset_handler.dataset_wikidata_qid)
        query = """
        INSERT INTO dataset (title, qid, collection)
        VALUES (%s, %s, %s)
        """
        title = dataset_handler.riksdagen_dataset_title
        done_query = self.cursor.mogrify(query, (title, item_int, None))
        print(done_query)
        self.cursor.execute(query, (title, item_int, dataset_handler.collection_id))
        self.commit_to_database()

    def add_document_to_database(self, document: Any):
        logger.info("Adding document to database")
        # Assuming 'documents' is the name of the table where documents are stored
        query = "INSERT IGNORE INTO document (external_id, dataset) VALUES (%s, %s)"
        values = (document.external_id, document.dataset_id)
        self.cursor.execute(query, values)
        self.commit_to_database()
        logger.info("Document added to the database.")

    def insert_rawtoken(self, token: Any):
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

    def insert_normtoken(self, token: Any):
        query = """
        INSERT IGNORE INTO normtoken 
        (text)
        VALUES (%s);
        """
        params = (token.normalized_token,)
        self.cursor.execute(query, params)
        self.commit_to_database()
        logger.info("normtoken inserted")

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
        # todo can we do this automatically?
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

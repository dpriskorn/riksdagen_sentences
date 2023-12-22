import logging
from typing import Any, List

from models.crud.database_handler import Mariadb
from models.exceptions import PostagError, MissingLanguageError

logger = logging.getLogger(__name__)


class Read(Mariadb):
    def connect_and_setup(self):
        self.connect_to_mariadb()
        self.initialize_mariadb_cursor()

    def get_all_dataset_ids(self) -> List[int]:
        """Return ids of all datasets"""
        query = """SELECT id
                    FROM dataset;
                """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        ids = [row[0] for row in result]
        # for row in result:
        #     print(row[0])
        logger.debug(ids)
        return ids

    def get_workdirectory(self, dataset: Any):
        query = """SELECT workdirectory
            FROM dataset
            WHERE id = %s;
        """
        params = (dataset.id,)
        logger.debug(self.cursor.mogrify(query, params))
        self.cursor.execute(query, params)
        result = self.cursor.fetchone()
        # print(result)
        if result:
            data = result[0]
            logger.info(f"Got dataset workdirectory: {data}")
            return data

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
        logger.debug(result)
        if result:
            dataset_id = [0]
            print(f"Got dataset id: {dataset_id}")
            return dataset_id

    # def get_rawtoken_with_certain_lexical_category(
    #     self, rawtoken: str, lexical_category: str
    # ):
    #     lexcat_item_int = self.item_int(lexical_category)
    #     query = """
    #         SELECT *
    #         FROM rawtoken
    #         JOIN lexical_category ON rawtoken.lexical_category_id = lexical_category.id
    #         WHERE rawtoken.text = %s
    #         AND rawtoken.lexical_category_id = %s
    #         """
    #     self.row_cursor.execute(query, (rawtoken, lexcat_item_int))
    #     results = self.row_cursor.fetchall()
    #     return results
    #
    # def get_sentences_for_rawtoken_id(self, rawtoken_id: int):
    #     query = """
    #     SELECT * FROM sentence
    #     JOIN rawtoken_sentence_linking ON sentence.id = rawtoken_sentence_linking.sentence_id
    #     WHERE rawtoken_sentence_linking.rawtoken_id = %s
    #     """
    #     self.row_cursor.execute(query, (rawtoken_id,))
    #     results = self.row_cursor.fetchall()
    #     return results
    #

    def get_lexical_category_id(self, token: Any) -> int:
        query = """SELECT id
            FROM lexical_category
            WHERE postag = %s;
        """
        self.cursor.execute(query, (token.pos,))
        result = self.cursor.fetchone()
        if not result:
            raise PostagError(f"postag {token.pos} not found in database")
        else:
            rowid = result[0]
        logger.info(f"Got lexical category rowid: {rowid}")
        return rowid

    def get_score(self, sentence: Any) -> int:
        query = """
            SELECT id
            FROM score
            WHERE ROUND(value, 2) = %s
        """
        params = (sentence.score,)
        logger.debug(self.cursor.mogrify(query, params))
        self.cursor.execute(query, params)
        result = self.cursor.fetchone()
        if result:
            score_id = result[0]
            logger.info(f"Got score id: {score_id}")
            return score_id

    def get_language(self, sentence: Any) -> int:
        code = sentence.detected_language.lower()
        query = """
            SELECT id
            FROM language
            WHERE iso_code = %s
        """
        params = (code,)
        self.cursor.execute(query, params)
        result = self.cursor.fetchone()
        if result:
            rowid = result[0]
            logger.info(f"Got language id: {rowid}")
            return rowid
        else:
            raise MissingLanguageError(f"iso_code {code} for sentence {sentence.text}")

    def get_document_id(self, document: Any) -> int:
        query = """
            SELECT id
            FROM document
            WHERE external_id = %s
        """
        params = (document.external_id,)
        self.cursor.execute(query, params)
        result = self.cursor.fetchone()
        if result:
            rowid = result[0]
            logger.info(f"Got document id: {rowid}")
            return rowid

    def get_sentence_id(self, sentence: Any) -> int:
        query = """
            SELECT id
            FROM sentence
            WHERE text = %s and document = %s and language = %s
        """
        params = (
            sentence.text,
            sentence.document.id,
            sentence.language_id,
        )
        self.cursor.execute(query, params)
        result = self.cursor.fetchone()
        if result:
            rowid = result[0]
            logger.info(f"Got sentence id: {rowid}")
            return rowid

    def get_normtoken_id(self, token: Any):
        query = """SELECT id
            FROM normtoken
            WHERE text = %s 
        """
        self.cursor.execute(query, (token.normalized_token,))
        result = self.cursor.fetchone()
        if result:
            rowid = result[0]
            logger.info(f"Got normtoken id: {rowid}")
            return rowid

    def get_rawtoken_id(self, token: Any):
        query = """SELECT id
            FROM rawtoken
            WHERE text = %s and lexical_category = %s;
        """
        self.cursor.execute(query, (token.rawtoken, token.pos_id))
        result = self.cursor.fetchone()
        if result:
            rowid = result[0]
            logger.info(f"Got rawtoken id: {rowid}")
            return rowid

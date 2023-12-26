import logging
from typing import Any, List, Tuple

import spacy

from models.api import SentenceResult
from models.api.sentence_result import SentenceAttributes
from models.crud.database_handler import Mariadb
from models.exceptions import PostagError, MissingLanguageError, MissingInformationError

logger = logging.getLogger(__name__)


class Read(Mariadb):
    """Read methods and helper methods"""

    @staticmethod
    def parse_into_sentence_results(results: Any) -> List[SentenceResult]:
        if results:
            sentence_results = list()
            for result in results:
                sentence_results.append(
                    SentenceResult(
                        attributes=SentenceAttributes(text=result[0], score=result[2]),
                        id=result[1],
                    )
                )
            return sentence_results
        else:
            return list()

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
        # logger.debug(self.cursor.mogrify(query, params))
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

    def get_rawtoken_id_with_specific_language_and_lexical_category(
        self, language: str, rawtoken: str, lexical_category: str
    ) -> int:
        query = """
        SELECT rawtoken.id
        FROM rawtoken
        JOIN lexical_category ON rawtoken.lexical_category = lexical_category.id
        JOIN language ON rawtoken.language = language.id
        WHERE rawtoken.text = %s
        AND lexical_category.qid = %s
        AND language.iso_code = %s;
        """
        self.cursor.execute(query, (rawtoken, lexical_category, language))
        result = self.cursor.fetchone()
        if result:
            return result[0]

    def count_sentences_for_rawtoken_without_space(self, rawtoken_id: int) -> int:
        query = """
        SELECT COUNT(sentence.id) AS sentence_count
        FROM sentence
        JOIN rawtoken_sentence_linking ON sentence.id = rawtoken_sentence_linking.sentence
        WHERE rawtoken_sentence_linking.rawtoken = %s
        """
        self.cursor.execute(query, (rawtoken_id,))
        result = self.cursor.fetchone()
        if result:
            return int(result[0])
        else:
            raise ValueError("Got no result for count")

    def count_sentences_for_compound_token(self, language: str, compound_token: str) -> int:
        query = """
        SELECT COUNT(sentence.id) AS sentence_count
        FROM sentence
        JOIN language ON sentence.language = language.id
        WHERE language.iso_code = %s
        AND LOWER(sentence.text) LIKE LOWER(%s)
        """
        self.cursor.execute(query, (language, f"%{compound_token}%"))
        result = self.cursor.fetchone()
        if result:
            return int(result[0])
        else:
            raise ValueError("Got no result for count")

    def get_sentences_for_rawtoken_without_space(
        self, rawtoken_id: int, limit: int = 100, offset: int = 0
    ) -> Tuple[int, List[SentenceResult]]:
        count = self.count_sentences_for_rawtoken_without_space(rawtoken_id=rawtoken_id)
        if count:
            query = """
            SELECT sentence.text, sentence.uuid, score.value as score_value
            FROM sentence
            JOIN rawtoken_sentence_linking ON sentence.id = rawtoken_sentence_linking.sentence
            JOIN score ON sentence.score = score.id
            WHERE rawtoken_sentence_linking.rawtoken = %s
            ORDER BY LENGTH(sentence.text) ASC
            LIMIT %s OFFSET %s;
            """
            self.cursor.execute(query, (rawtoken_id, limit, offset))
            results = self.cursor.fetchall()
            return count, self.parse_into_sentence_results(results=results)
        else:
            return count, list()

    def get_sentences_for_compound_token(
        self, compound_token: str, language: str, limit: int = 100, offset: int = 0
    ) -> Tuple[int, List[SentenceResult]]:
        """This is case-insensitive"""
        count = self.count_sentences_for_compound_token(language=language, compound_token=compound_token)
        if count:
            query = """
            SELECT sentence.text, sentence.uuid, score.value
            FROM sentence
            JOIN language ON sentence.language = language.id
            JOIN score ON sentence.score = score.id
            WHERE language.iso_code = %s
            AND LOWER(sentence.text) LIKE LOWER(%s)
            ORDER BY LENGTH(sentence.text) ASC
            LIMIT %s OFFSET %s;
            """
            self.cursor.execute(query, (language, f"%{compound_token}%", limit, offset))
            results = self.cursor.fetchall()
            return count, self.parse_into_sentence_results(results=results)
        else:
            logger.info("Count was 0")
            return count, list()

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
        logger.debug(f"Got lexical category rowid: {rowid}")
        return rowid

    def get_score(self, sentence: Any) -> int:
        query = """
            SELECT id
            FROM score
            WHERE ROUND(value, 2) = %s
        """
        params = (sentence.score,)
        # logger.debug(self.cursor.mogrify(query, params))
        self.cursor.execute(query, params)
        result = self.cursor.fetchone()
        if result:
            score_id = result[0]
            logger.debug(f"Got score id: {score_id}")
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
            logger.debug(f"Got language id: {rowid}")
            return rowid
        else:
            raise MissingLanguageError(
                f"iso_code {code}:\n"
                f"score: {sentence.score}\n"
                f"clean word count: {sentence.number_of_words_in_clean_sentence}\n"
                f"cleaned sentence '{sentence.cleaned_sentence}'\n"
                f"raw sentence '{sentence.text}"
            )

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
            logger.debug(f"Got document id: {rowid}")
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
            logger.debug(f"Got sentence id: {rowid}")
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
            logger.debug(f"Got normtoken id: {rowid}")
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
            logger.debug(f"Got rawtoken id: {rowid}")
            return rowid

    def get_processed_status(self, document) -> bool:
        query = """SELECT processed
                    FROM document
                    WHERE id = %s;
                """
        self.cursor.execute(query, (document.id,))
        result = self.cursor.fetchone()
        if result:
            rowid = result[0]
            logger.debug(f"Got processed status: {rowid}")
            return bool(rowid)

    def get_ner_label_id(self, entity: Any):
        query = """SELECT id
        FROM ner_label
        WHERE label = %s;
        """
        self.cursor.execute(query, (entity.ner_label,))
        result = self.cursor.fetchone()
        if result:
            rowid = result[0]
            logger.debug(f"Got ner_label id: {rowid}")
            return rowid
        else:
            raise MissingInformationError(
                f"ner label '{entity.ner_label}' "
                f"not found, description: '{spacy.explain(entity.ner_label)}'"
            )

    def get_entity_id(self, entity: Any) -> int:
        query = """SELECT id
                FROM entity
                WHERE label = %s and ner_label = %s;
                """
        self.cursor.execute(query, (entity.label, entity.ner_label_id))
        result = self.cursor.fetchone()
        if result:
            rowid = result[0]
            logger.debug(
                f"Got entity id: {rowid} for {entity.label}:{entity.ner_label_id}"
            )
            return rowid
        else:
            logger.debug(f"No entity found for {entity.label}:{entity.ner_label_id}")

    def get_all_iso_codes(self) -> List[str]:
        self.cursor.execute("SELECT iso_code FROM language")
        iso_codes = [row[0] for row in self.cursor.fetchall()]
        return iso_codes

    def get_all_lexical_language_qids(self):
        self.cursor.execute("SELECT qid FROM lexical_category")
        qids = [row[0] for row in self.cursor.fetchall()]
        return qids

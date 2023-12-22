import json
import logging
import os
from typing import Any

from pydantic import BaseModel

from models.crud.read import Read
from models.document import Document

logger = logging.getLogger(__name__)


class Dataset(BaseModel):
    # todo avoid hardcoding for riksdagen
    # collection_id: int = 0  # hardcoded for now
    id: int
    analyzer: Any = None
    max_documents_to_extract_per_dataset: int = 0

    @property
    def dataset_title(self):
        raise NotImplementedError()

    # @property
    # def dataset_id(self) -> int:
    #     return self.analyzer.mariadb.get_dataset_id(dataset_handler=self)

    @property
    def workdirectory(self) -> str:
        read = Read()
        read.connect_and_setup()
        data = read.get_workdirectory(dataset=self)
        read.close_db()
        return data

    # @property
    # def qid(self) -> str:
    #     return config.supported_document_types[self.riksdagen_dataset_title][
    #         "wikidata_qid"
    #     ]

    def analyze(self):
        self.__read_json_from_disk_and_extract()
        self.print_number_of_skipped_documents()
        self.print_number_of_tokens()

    def __read_json_from_disk_and_extract(self):
        logger.info("reading json from disk")
        if not self.workdirectory:
            raise ValueError("workdirectory was empty string")
        file_paths = []
        for root, dirs, files in os.walk(self.workdirectory):
            for file in files:
                if file.endswith(".json"):
                    file_paths.append(os.path.join(root, file))

        # logger.info(f"Number of filepaths found: {len(file_paths)}")
        # logger.info(f"Number of filepaths after offset: {len(file_paths)}")
        count = 1
        for file_path in file_paths:
            # Only break if max_documents_to_extract is different from 0
            if (
                self.max_documents_to_extract_per_dataset
                and count >= self.max_documents_to_extract_per_dataset
            ):
                print("Max documents limit reached.")
                break
            with open(file_path, "r", encoding="utf-8-sig") as json_file:
                # if count % 10 == 0 or count == 1:
                print(f"Processing document {count}/{len(file_paths)}")
                try:
                    data = json.load(json_file)
                    if (
                        "dokumentstatus" in data
                        and "dokument" in data["dokumentstatus"]
                    ):
                        dok_id = data["dokumentstatus"]["dokument"].get("dok_id")
                        text = data["dokumentstatus"]["dokument"].get("text")
                        html = data["dokumentstatus"]["dokument"].get("html")

                        if dok_id is not None and (
                            text is not None or html is not None
                        ):
                            # We got a good document with content
                            document = Document(
                                external_id=dok_id,
                                dataset_id=self.id,
                                text=text or "",
                                html=html or "",
                            )
                            document.insert_extract_and_update()
                        else:
                            self.skipped_documents_count += 1
                            logger.info(
                                f"Skipping document {json_file}: Missing dok_id and (text or html)"
                            )
                    else:
                        logger.info(
                            f"Skipping document {json_file}: Missing 'dokumentstatus' or 'dokument'"
                        )
                except json.JSONDecodeError as e:
                    logger.error(f"Error loading JSON from {file_path}: {e}")
                count = +1

    # def print_number_of_documents(self):
    #     # Print or use the variable containing all text
    #     print(f"number of documents: {len(self.documents)}")
    #
    # def print_number_of_tokens(self):
    #     # Print or use the variable containing all text
    #     print(f"Total number of tokens: {self.token_count}")

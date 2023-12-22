import json
import logging
import os
from typing import Any

from pydantic import BaseModel
from tqdm import tqdm

from models.crud.insert import Insert
from models.crud.read import Read
from models.riksdagen_document import RiksdagenDocument

logger = logging.getLogger(__name__)


class Dataset(BaseModel):
    # collection_id: int = 0  # hardcoded for now
    id: int
    analyzer: Any = None
    # todo decide whether to remove or support this
    document_offset: int = 0
    max_documents_to_extract_per_dataset: int = 2

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
    #     return config.supported_riksdagen_document_types[self.riksdagen_dataset_title][
    #         "wikidata_qid"
    #     ]

    def read_json_from_disk_and_extract(self):
        logger.info("reading json from disk")
        if not self.workdirectory:
            raise ValueError("workdirectory was empty string")
        file_paths = []
        for root, dirs, files in os.walk(self.workdirectory):
            for file in files:
                if file.endswith(".json"):
                    file_paths.append(os.path.join(root, file))

        logger.info(f"Number of filepaths found: {len(file_paths)}")

        # Handle offset
        file_paths = file_paths[self.document_offset :]
        # print(file_paths[:1])
        # exit()
        logger.info(f"Number of filepaths after offset: {len(file_paths)}")

        # Wrap the iteration with tqdm to display a progress bar
        count = 0
        for file_path in tqdm(file_paths, desc="Processing JSON files"):
            # Only break if max_documents_to_extract is different from 0
            if self.max_documents_to_extract_per_dataset and count >= self.max_documents_to_extract_per_dataset:
                print("Max documents limit reached.")
                break
            with open(file_path, "r", encoding="utf-8-sig") as json_file:
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
                            document = RiksdagenDocument(
                                external_id=dok_id,
                                dataset_id=self.id,
                                text=text or "",
                                html=html or "",
                            )
                            insert = Insert()
                            insert.connect_and_setup()
                            insert.add_document_to_database(document=document)
                            insert.close_db()
                            document.extract_sentences()
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

from typing import Dict, List

import yaml
from pydantic import BaseModel

from models.crud.insert import Insert
from models.crud.read import Read
from models.dataset import Dataset


class Datasets(BaseModel):
    datasets: List[Dataset] = list()
    raw_datasets: Dict[str, str] = dict()
    datasets_config_path: str = "config/datasets.yml"

    def setup(self):
        self.load_languages_from_yaml()
        self.insert_datasets()
        self.get_datasets()

    def load_languages_from_yaml(self):
        # Load YAML into a dictionary
        with open(self.datasets_config_path, "r") as file:
            # Read YAML content from the file
            self.raw_datasets = yaml.safe_load(file)

    def insert_datasets(self):
        insert = Insert()
        insert.connect_and_setup()
        insert.insert_datasets_in_database(datasets=self)
        insert.close_db()

    def get_datasets(self):
        read = Read()
        read.connect_and_setup()
        result = read.get_all_dataset_ids()
        for id in result:
            dataset = Dataset(id=id)
            self.datasets.append(dataset)

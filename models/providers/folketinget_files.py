from typing import List

import requests
from pydantic import BaseModel

from models.providers.folketinget_file import FolketingetFile


class FolketingetFiles(BaseModel):
    """Class that handles downloading of Fil object json from Folketinget

    Inspired by https://towardsdatascience.com/extracting-text-from-pdf-files-with-python-a-comprehensive-guide-9fc4003d517
    """

    url: str
    files: List[FolketingetFile] = list()

    def start(self):
        print("Downloading from Folketinget")
        # TODO turn into a generator to yield all documents
        #  url: https://oda.ft.dk/api/Fil?$inlinecount=allpages&$skip=100
        json_ = self.fetch_and_parse_json()
        self.parse_into_objects(json_data=json_)
        for file in self.files:
            print(file.model_dump())
            file.download_and_extract_and_save_to_disk()

    def fetch_and_parse_json(self):
        try:
            response = requests.get(self.url)
            if response.status_code == 200:
                json_data = response.json()
                # Process the JSON data here
                return json_data
            else:
                print(f"Failed to fetch data. Status code: {response.status_code}")
                return None
        except requests.RequestException as e:
            print(f"Request Exception: {e}")
            return None

    def parse_into_objects(self, json_data):
        if json_data is None or "value" not in json_data:
            print("Invalid JSON data or missing 'value' key.")
            return []

        values = json_data["value"]
        for item in values:
            file = FolketingetFile(
                id=item.get("id"),
                dokumentid=item.get("dokumentid"),
                titel=item.get("titel"),
                versionsdato=item.get("versionsdato"),
                variantkode=item.get("variantkode"),
                filurl=item.get("filurl"),
            )
            self.files.append(file)

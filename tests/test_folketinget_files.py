from unittest import TestCase

from models.providers.folketinget_files import FolketingetFiles


class TestFolketingetFiles(TestCase):
    def test_start(self):
        # Usage example
        folketinget_instance = FolketingetFiles(
            url="https://oda.ft.dk/api/Fil?$inlinecount=allpages"
        )
        folketinget_instance.start()

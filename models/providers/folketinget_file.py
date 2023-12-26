import hashlib
import json
import os
import requests
from pydantic import BaseModel
from io import BytesIO
from pdfminer.high_level import extract_text
from pdfminer.pdfparser import PDFSyntaxError


class FolketingetFile(BaseModel):
    """This downloads and extract the data in the Fil objects from Folketinget"""
    id: int
    dokumentid: int
    titel: str
    versionsdato: str
    variantkode: str
    filurl: str
    metadata_directory: str = "data/da/folketinget"
    text_directory: str = "data/da/folketinget/txt"
    pdf_directory: str = "data/da/folketinget/pdf"
    pdf_content: bytes = None

    @property
    def md5_hash(self):
        """We use the filurl for now to generate the hash
        because we don't know if any of the other things are unique"""
        return hashlib.md5(self.filurl.encode()).hexdigest()

    @property
    def pdf_filename(self):
        return f"{self.md5_hash}.pdf"

    @property
    def text_filename(self):
        return f"{self.md5_hash}.txt"

    @property
    def already_downloaded(self):
        pdf_path = os.path.join(self.pdf_directory, self.pdf_filename)
        text_path = os.path.join(self.text_directory, self.text_filename)
        return os.path.exists(pdf_path) and os.path.exists(text_path)

    def download_and_extract_and_save_to_disk(self):
        if not self.already_downloaded:
            self.save_pdf()
            self.save_text()
            self.save_metadata()
        else:
            print("Skipping already downloaded FolketingetFile object")

    def save_metadata(self):
        metadata_filename = f"metadata.jsonl"  # File containing JSON lines
        metadata_path = os.path.join(self.metadata_directory, metadata_filename)

        metadata = {
            "id": self.id,
            "dokumentid": self.dokumentid,
            "titel": self.titel,
            "versionsdato": self.versionsdato,
            "variantkode": self.variantkode,
            "filurl": self.filurl,
        }

        # Check if metadata file exists or create it
        if not os.path.exists(metadata_path):
            with open(metadata_path, "w", encoding="utf-8") as new_metadata_file:
                new_metadata_file.write(json.dumps(metadata, ensure_ascii=False) + "\n")
            print(f"Metadata file created at: {metadata_path}")
        else:
            with open(metadata_path, "a", encoding="utf-8") as metadata_file:
                metadata_file.write(json.dumps(metadata, ensure_ascii=False) + "\n")
            print(f"Metadata appended to: {metadata_path}")

        print(f"Metadata appended to: {metadata_path}")

    def fetch_and_check_pdf(self) -> None:
        if self.pdf_content is None:
            try:
                response = requests.get(self.filurl, stream=True)
                if (
                    response.status_code == 200
                    and response.headers["content-type"] == "application/pdf"
                ):
                    print("Got PDF content from URL")
                    self.pdf_content = response.content
                else:
                    print(f"Failed to fetch PDF from URL: {self.filurl}")
            except requests.RequestException as e:
                print(f"Request Exception: {e}")

    def extract_pdf_text(self):
        self.fetch_and_check_pdf()
        if self.pdf_content is not None:
            try:
                with BytesIO(self.pdf_content) as pdf_buffer:
                    text = extract_text(pdf_buffer)
                    return text
            except PDFSyntaxError as e:
                print(f"PDF Syntax Error: {e}")
                return None
        else:
            print("No valid PDF content to extract.")
            return None

    def save_pdf(self):
        self.fetch_and_check_pdf()
        directory = self.pdf_directory
        if self.pdf_content:
            pdf_path = os.path.join(directory, f"{self.md5_hash}.pdf")
            with open(pdf_path, "wb") as pdf_file:
                pdf_file.write(self.pdf_content)
            print(f"PDF saved at: {pdf_path}")
        else:
            print("No valid PDF content to save.")

    def save_text(self):
        directory = self.text_directory
        extracted_text = self.extract_pdf_text()
        if extracted_text:
            text_path = os.path.join(directory, f"{self.md5_hash}.txt")
            with open(text_path, "w", encoding="utf-8") as text_file:
                text_file.write(extracted_text)
            print(f"Text extracted and saved at: {text_path}")
        else:
            print("No text extracted or saved.")

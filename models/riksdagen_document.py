from typing import List

import spacy
from pydantic import BaseModel


class RiksdagenDocument(BaseModel):
    id: str
    text: str
    chunk_size: int = 100000
    chunks: List[str] = list()
    sentences: List[str] = list()

    @property
    def count_words(self) -> int:
        # Counting words in the text
        return len(self.text.split())

    def chunk_text(self):
        # Function to chunk the text
        start = 0
        while start < len(self.text):
            self.chunks.append(self.text[start: start + self.chunk_size])
            start += self.chunk_size

    @property
    def number_of_chunks(self) -> int:
        # Count the number of chunks
        return len(self.chunks)

    @property
    def number_of_sentences(self) -> int:
        # Count the number of chunks
        return len(self.sentences)

    def extract_sentences(self):
        # Load the Swedish language model
        nlp = spacy.load("sv_core_news_sm")

        # Displaying the word count
        print(f"Number of words before tokenization: {self.count_words}")

        # Chunk the text
        self.chunk_text()

        # Display the number of chunks
        print(f"Number of chunks: {self.number_of_chunks}")

        # Process each chunk separately
        count = 1
        for chunk in self.chunks:
            print(f"loading chunk {count}/{self.number_of_chunks}")
            doc = nlp(chunk)

            # Filter out sentences consisting only of newline characters
            filtered_sentences = [sent.text for sent in doc.sents if sent.text.strip()]
            self.sentences.extend(filtered_sentences)

            count += 1

        print(f"Extracted {len(self.sentences)} sentences")

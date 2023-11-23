import argparse
import hashlib
import json
import logging
import lzma
import os
import re
import string
import uuid
from typing import List

import jsonlines
from langdetect import detect
from pandas import DataFrame
from pydantic import BaseModel
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm

import config
from models.riksdagen_document import RiksdagenDocument

logger = logging.getLogger(__name__)


class RiksdagenAnalyzer(BaseModel):
    """This model extracts sentences from a supported riksdagen document type
    and stores the result in both jsonl and pickle formats."""

    riksdagen_document_type: str = ""
    documents: List[RiksdagenDocument] = []
    df: DataFrame = DataFrame()
    max_documents_to_extract: int = 0  # zero means no limit
    skipped_documents_count: int = 0
    document_offset: int = 0
    token_count: int = 0
    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    jsonl_path_to_load: str = ""
    arguments: argparse.Namespace = argparse.Namespace()
    additional_stop_words: List[str] = [
        "ska",
        "enligt",
        "även",
        "samt",
        "finns",
        "får",
        "också",
        "kap",
        "vis",
        "andra",
        "genom",
        "innebär",
        "in",
        "dock",
        "rätt",
        "ds",
        "d",
        "bör",
    ]

    class Config:
        arbitrary_types_allowed = True

    @property
    def workdirectory(self) -> str:
        return config.supported_riksdagen_document_types[self.riksdagen_document_type][
            "workdirectory"
        ]

    @property
    def filename(self):
        return config.supported_riksdagen_document_types[self.riksdagen_document_type][
            "filename"
        ]

    def start_analyzing(self):
        self.read_json_from_disk_and_extract()
        # self.print_number_of_documents()
        self.print_number_of_skipped_documents()
        self.print_number_of_tokens()
        # self.extract_sentences_from_all_documents()
        # self.create_dataframe_with_all_sentences()
        # self.generate_ids()
        # self.strip_newlines()
        # self.determine_suitability()
        # self.determine_language()
        # self.print_statistics()
        # self.save()
        # self.generate_document_term_matix()

    def handle_arguments(self):
        self.setup_argparse()
        self.arguments = self.parser.parse_args()
        if self.arguments.load_jsonl:
            self.load_jsonl()
        if self.arguments.max:
            self.max_documents_to_extract = self.arguments.max
        if self.arguments.offset:
            self.document_offset = self.arguments.offset
        if self.arguments.analyze:
            self.riksdagen_document_type = self.arguments.analyze
            self.start_analyzing()

    def load_jsonl(self):
        """Load jsonl from disk to be able to work on it"""
        print("JSONL file will be loaded.")
        if "xz" in self.arguments.load_jsonl:
            # Decompress the xz file
            with lzma.open(self.arguments.load_jsonl, "rb") as compressed_file:
                # Read the decompressed JSONL data
                decompressed_data = compressed_file.read().decode("utf-8")

            # Convert the JSON lines data into a DataFrame
            self.df = pd.read_json(decompressed_data, lines=True)
        else:
            self.df = pd.read_json(self.arguments.load_jsonl, lines=True)

    def print_number_of_skipped_documents(self):
        print(
            f"Number of skipped JSON files "
            f"(because of missing or bad data): {self.skipped_documents_count}"
        )

    @staticmethod
    def detect_language(text):
        word_count = len(text.split())  # Split by spaces and count words
        if word_count > 5:
            try:
                return detect(text)
            except:
                return "Unknown"  # Handle cases where language detection fails
        else:
            return "Less than 5 words"

    def determine_language(self):
        # TODO can this be parallelized to use more than 1 cpu?
        print("Determining language for suitable sentences")
        suitable_sentences = self.df[self.df["suitable"]]["sentence"]

        # Use tqdm to show progress while applying language detection
        for idx in tqdm(
            suitable_sentences.index,
            desc="Detecting language",
            total=len(suitable_sentences),
        ):
            self.df.at[idx, "langdetect"] = self.detect_language(
                self.df.at[idx, "sentence"]
            )

    @staticmethod
    def create_plot(name: str, df: DataFrame):
        # Create a DataFrame of word frequencies
        word_freq = df

        # Sum the frequencies of each word
        word_freq_sum = word_freq.sum()

        # Sort words by frequency in descending order
        word_freq_sum = word_freq_sum.sort_values(ascending=False)

        # Plotting the top 10 most frequent words

        top_words = word_freq_sum.head(10)
        top_words.plot(kind="bar", figsize=(10, 6))
        plt.title("Top 10 Most Frequent Words")
        plt.xlabel("Words")
        plt.ylabel("Frequency")
        # fix for readability:
        plt.xticks(
            rotation=45, ha="right"
        )  # Rotate x-axis labels for better readability
        plt.tight_layout()
        plt.subplots_adjust(bottom=0.3)  # Increase space below bars
        # Save the plot to disk
        plt.savefig(name)
        # plt.show()

    def generate_document_term_matix(self):
        print("creating doucment-term matrix of suitable sentences")
        # We are going to create a document-term matrix using CountVectorizer, and exclude common English stop words
        from sklearn.feature_extraction.text import CountVectorizer
        from stop_words import get_stop_words

        # print(stop_words)

        stop_words = get_stop_words("sv")
        stop_words.extend(self.additional_stop_words)
        # Filter the DataFrame to get sentences where 'suitable' is True
        suitable_sentences = self.df[self.df["suitable"]]["sentence"]

        # Apply a first round of text cleaning techniques
        def clean_text_round1(text):
            """Make text lowercase, remove text in square brackets, remove punctuation and remove words containing numbers."""
            text = text.lower()
            text = re.sub("\[.*?\]", "", text)
            text = re.sub("[%s]" % re.escape(string.punctuation), "", text)
            text = re.sub("\w*\d\w*", "", text)
            return text

        round1 = lambda x: clean_text_round1(x)

        # Let's take a look at the updated text
        data_clean = pd.DataFrame(suitable_sentences.apply(round1))
        print(data_clean)

        # Create a trigram vectorizer
        print("calculating trigrams")
        trigram_vectorizer = CountVectorizer(ngram_range=(3, 3), stop_words=stop_words)
        data_cv_trigram = trigram_vectorizer.fit_transform(data_clean.sentence)
        data_dtm_trigram = pd.DataFrame(
            data_cv_trigram.toarray(),
            columns=trigram_vectorizer.get_feature_names_out(),
        )
        # data_dtm_trigram.to_csv("trigram_document-term_matrix.csv")

        # Create a bigram vectorizer
        print("calculating bigrams")
        bigram_vectorizer = CountVectorizer(ngram_range=(2, 2), stop_words=stop_words)
        data_cv_bigram = bigram_vectorizer.fit_transform(data_clean.sentence)
        data_dtm_bigram = pd.DataFrame(
            data_cv_bigram.toarray(), columns=bigram_vectorizer.get_feature_names_out()
        )
        # data_dtm_bigram.to_csv("bigram_document-term_matrix.csv")

        # monogram vectorizer
        print("calculating monograms")
        cv = CountVectorizer(stop_words=stop_words)
        data_cv = cv.fit_transform(data_clean.sentence)
        data_dtm = pd.DataFrame(data_cv.toarray(), columns=cv.get_feature_names_out())
        print(data_dtm)
        # data_dtm.to_csv("monogram_document-term_matrix.csv")

        # def create_plots(self):
        print("creating plots of most frequent words")

        self.create_plot(name="top_10_monogram.png", df=data_dtm)
        self.create_plot(name="top_10_bigram.png", df=data_dtm_bigram)
        self.create_plot(name="top_10_trigram.png", df=data_dtm_trigram)

        print("creating wordcloud")
        from wordcloud import WordCloud

        # Join the cleaned sentences into a single string
        text = " ".join(data_clean.sentence)
        text_without_stopwords = " ".join(
            [word for word in text.split() if word.lower() not in stop_words]
        )

        # Generate a word cloud
        wordcloud = WordCloud(width=800, height=400, background_color="white").generate(
            text_without_stopwords
        )

        # Display the word cloud using matplotlib
        plt.figure(figsize=(10, 6))
        plt.imshow(wordcloud, interpolation="bilinear")
        plt.axis("off")  # Turn off axis numbers
        plt.title("Word Cloud of Most Frequent Words")
        plt.savefig("wordcloud.png")

    @staticmethod
    def suitable_sentence(sentence):
        # Removing punctuation
        sentence_without_punctuation = "".join(
            char for char in sentence if char not in string.punctuation
        )

        # Split the sentence into words and remove words containing numbers
        words = [
            word
            for word in sentence_without_punctuation.split()
            if not any(char.isdigit() for char in word)
        ]

        # Check if the sentence has more than 5 words after removing numeric words
        if len(words) > 5:
            return True
        else:
            return False

    def determine_suitability(self):
        print("determining suitability")
        # Apply the suitable_sentence function to the 'sentences' column
        self.df["suitable"] = self.df["sentence"].apply(self.suitable_sentence)

    def strip_newlines(self):
        # Remove newlines from the end of sentences in the 'sentences' column
        self.df["sentence"] = self.df["sentence"].astype(str).str.rstrip("\n")

    def print_statistics(self):
        print("Statistics:")
        # Counting total number of rows
        total_sentences = self.df.shape[0]
        print("Total number of sentences:", total_sentences)

        # Count the number of empty sentences
        empty_sentences_count = (self.df["sentence"].str.strip() == "").sum()
        print(f"Number of empty sentences: {empty_sentences_count}")

        # Counting rows where 'suitable' column is True
        suitable_count = self.df[self.df["suitable"] == True].shape[0]
        print("Number of suitable sentences:", suitable_count)
        suitable_percentage = (suitable_count / total_sentences) * 100
        print("Percentage suitable sentences:", round(suitable_percentage))

        # Tokens
        try:
            total_tokens = self.df["tokens"].sum()
            print("Total sum of 'tokens' column:", total_tokens)
            # Calculate the mean number of tokens per sentence
            mean_tokens_per_sentence = total_tokens / total_sentences
            print("Mean number of tokens per sentence:", mean_tokens_per_sentence)
        except KeyError:
            pass

        # Counting and displaying the distribution of language codes
        language_distribution = self.df["langdetect"].value_counts()
        # Calculating percentages
        try:
            en_percentage = (language_distribution["en"] / total_sentences) * 100
            sv_percentage = (language_distribution["sv"] / total_sentences) * 100
            print("Percentage of 'en' (English) sentences:", round(en_percentage))
            print("Percentage of 'sv' (Swedish) sentences:", round(sv_percentage))
            print("Language code distribution:")
            print(language_distribution)
        except KeyError:
            pass

    def save(self):
        # self.df.to_pickle(f"{self.filename}.pickle.xz", compression="xz")
        # self.df.to_csv(f"{self.filename}.csv.xz", compression="xz")
        self.append_to_jsonl()

        # Save a version with only rows where 'suitable' is True
        # suitable_rows = self.df[self.df["suitable"]]
        # suitable_rows.to_pickle(f"{self.filename}_suitable.pickle.xz", compression="xz")
        # suitable_rows.to_csv(f"{self.filename}_suitable.csv.xz", compression="xz")
        # self.save_as_jsonl(suitable_rows, suitable=True)

    def create_suitable_jsonl(self):
        # TODO go through the jsonl line by line and save all lines with suitable=True to a new file
        pass

    def append_to_jsonl(self):
        # Assuming your DataFrame is named df
        # Replace 'your_data.jsonl' with the desired filename

        # Convert DataFrame to list of dictionaries
        data = self.df.to_dict(orient="records")

        # if suitable:
        #     filename = f"{self.filename}_suitable.jsonl"
        # else:
        filename = f"{self.filename}.jsonl"

        # Write data to a JSONL file
        with jsonlines.open(filename, mode="a") as writer:
            writer.write_all(data)

    def compress_jsonl(self):
        # Compress the JSONL file with xz
        with open(f"{self.filename}.jsonl", "rb") as jsonl_file:
            with lzma.open(f"{self.filename}.jsonl.xz", "wb") as xz_file:
                for line in jsonl_file:
                    xz_file.write(line)

    # def remove_uncompressed_jsonl(self):
    #     os.remove(self.filename)

    @staticmethod
    def generate_md5_hash(sentence):
        return hashlib.md5(sentence.encode()).hexdigest()

    def generate_id_and_hash(self):
        # Generate UUIDs for each sentence and add them to a new 'uuid' column
        self.df["uuid"] = [str(uuid.uuid4()) for _ in range(len(self.df))]

        # Generate MD5 hash for each sentence and add them to a new 'md5_hash' column

        self.df["md5_hash"] = self.df["sentence"].apply(self.generate_md5_hash)

    # def extract_sentences_from_all_documents(self):
    #     total_documents = min(len(self.documents), self.max_documents_to_extract)
    #     with tqdm(
    #         total=total_documents, desc="Extracting sentences from all documents", unit="doc"
    #     ) as pbar_docs:
    #         for index, doc in enumerate(self.documents, 1):
    #             if index > self.max_documents_to_extract:
    #                 print("max reached, stopping extraction")
    #                 break
    #             pbar_docs.update(1)
    #             doc.extract_sentences()

    def create_dataframe_with_all_sentences(self):
        print("creating dataframe")
        # Creating DataFrame
        data = {"id": [], "sentence": [], "tokens": 0}

        for doc in self.documents:
            for sentence in doc.sentences:
                data["id"].append(doc.id)
                data["sentence"].append(sentence.text)
                data["tokens"] = sentence.token_count

        self.df = pd.DataFrame(data)

    def dataframe_is_empty(self) -> bool:
        return self.df.empty

    def read_json_from_disk_and_extract(self):
        # print("reading json from disk")
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
            if self.max_documents_to_extract and count >= self.max_documents_to_extract:
                logger.info("Max documents limit reached.")
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
                            document = RiksdagenDocument(
                                id=dok_id, text=text or "", html=html or ""
                            )
                            document.extract_sentences()
                            self.token_count = +document.token_count
                            self.print_number_of_tokens()
                            self.documents.append(document)
                            self.create_dataframe_with_all_sentences()
                            if not self.dataframe_is_empty():
                                self.generate_id_and_hash()
                                self.strip_newlines()
                                self.determine_suitability()
                                self.determine_language()
                                self.append_to_jsonl()
                            else:
                                logger.warning(
                                    f"Document with id {document.id} with path "
                                    f"{file_path} did not have any sentences"
                                )
                                self.skipped_documents_count += 1
                            # Reset documents to avoid getting killed by the
                            # kernel because we run out of memory
                            self.documents = []
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

    def print_number_of_documents(self):
        # Print or use the variable containing all text
        print(f"number of documents: {len(self.documents)}")

    def print_number_of_tokens(self):
        # Print or use the variable containing all text
        print(f"Total number of tokens: {self.token_count}")

    def setup_argparse(self):
        self.parser = argparse.ArgumentParser(description="Parse JSONL file")
        self.parser.add_argument(
            "-l", "--load-jsonl", type=str, help="Load JSONL file", required=False
        )
        self.parser.add_argument(
            "--offset", type=int, help="Document offset", required=False
        )
        self.parser.add_argument(
            "--max", type=int, help="Max documents to process", required=False
        )
        self.parser.add_argument(
            "--analyze",
            type=str,
            help="Analyze a document series. One of ['departementserien', 'proposition']",
            required=False,
        )

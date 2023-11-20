import hashlib
import json
import os
import uuid
from typing import List

from langdetect import detect
from pandas import DataFrame
from pydantic import BaseModel
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm

from models.riksdagen_document import RiksdagenDocument


class RiksdagenAnalyzer(BaseModel):
    # Replace 'path/to/your/directory' with the path to your directory containing the .txt files
    language_directory: str = "data/sv"
    documents: List[RiksdagenDocument] = []
    df: DataFrame = DataFrame()
    max_documents_to_extract: int = 1000
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
        "bör"
    ]

    class Config:
        arbitrary_types_allowed = True

    def start(self):
        self.read_json_from_disk()
        self.print_number_of_documents()
        self.extract_sentences_from_all_documents()
        self.create_dataframe_with_all_sentences()
        self.generate_ids()
        self.strip_newlines()
        self.determine_suitability()
        # print(self.df)
        # exit()
        self.determine_language()
        self.save_and_print()
        # self.generate_document_term_matix()

    @staticmethod
    def detect_language(text):
        word_count = len(text.split())  # Split by spaces and count words
        if word_count > 5:
            try:
                return detect(text)
            except:
                return 'Unknown'  # Handle cases where language detection fails
        else:
            return 'Less than 5 words'

    def determine_language(self):
        print("determining language for suitable sentences")
        # Apply language detection only for rows where Suitable_Column is True
        self.df['langdetect'] = self.df[self.df['suitable']]['sentence'].apply(self.detect_language)

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
        plt.xticks(rotation=45, ha="right")  # Rotate x-axis labels for better readability
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
        import re
        import string

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
        data_dtm_trigram = pd.DataFrame(data_cv_trigram.toarray(), columns=trigram_vectorizer.get_feature_names_out())
        # data_dtm_trigram.to_csv("trigram_document-term_matrix.csv")

        # Create a bigram vectorizer
        print("calculating bigrams")
        bigram_vectorizer = CountVectorizer(ngram_range=(2, 2), stop_words=stop_words)
        data_cv_bigram = bigram_vectorizer.fit_transform(data_clean.sentence)
        data_dtm_bigram = pd.DataFrame(data_cv_bigram.toarray(), columns=bigram_vectorizer.get_feature_names_out())
        # data_dtm_bigram.to_csv("bigram_document-term_matrix.csv")

        # monogram vectorizer
        print("calculating monograms")
        cv = CountVectorizer(stop_words=stop_words)
        data_cv = cv.fit_transform(data_clean.sentence)
        data_dtm = pd.DataFrame(data_cv.toarray(), columns=cv.get_feature_names_out())
        print(data_dtm)
        # data_dtm.to_csv("monogram_document-term_matrix.csv")


    #def create_plots(self):
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
        # Split the sentence into words and remove words containing numbers
        words = [word for word in sentence.split() if not any(char.isdigit() for char in word)]

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

    def save_and_print(self):
        # Display the DataFrame
        print(self.df)
        print(self.df.info())

        # Count the number of empty sentences
        empty_sentences_count = (self.df['sentence'].str.strip() == '').sum()

        print(f"Number of empty sentences: {empty_sentences_count}")

        self.df.to_excel("departementserien.xlsx")
        self.df.to_pickle("departementserien.pickle.xz", compression="xz")

    @staticmethod
    def generate_md5_hash(sentence):
        return hashlib.md5(sentence.encode()).hexdigest()

    def generate_ids(self):
        # Generate UUIDs for each sentence and add them to a new 'uuid' column
        self.df['uuid'] = [str(uuid.uuid4()) for _ in range(len(self.df))]

        # Generate MD5 hash for each sentence and add them to a new 'md5_hash' column

        self.df['md5_hash'] = self.df['sentence'].apply(self.generate_md5_hash)

    def extract_sentences_from_all_documents(self):
        print("Extracting sentences from all documents")
        total_documents = min(len(self.documents), self.max_documents_to_extract)
        with tqdm(total=total_documents, desc="Processing Documents", unit="doc") as pbar_docs:
            for index, doc in enumerate(self.documents, 1):
                if index > self.max_documents_to_extract:
                    print("max reached, stopping extraction")
                    break
                pbar_docs.update(1)
                tqdm.write(f"Processing document {index}/{len(self.documents)}")
                doc.extract_sentences()

    def create_dataframe_with_all_sentences(self):
        print("creating dataframe")
        # Creating DataFrame
        data = {
            'id': [],
            'sentence': []
        }

        for doc in self.documents:
            for sentence in doc.sentences:
                data['id'].append(doc.id)
                data['sentence'].append(sentence)

        self.df = pd.DataFrame(data)

    def read_json_from_disk(self):
        print("reading json from disk")
        for root, dirs, files in os.walk(self.language_directory):
            for file in files:
                if file.endswith(".json"):
                    file_path = os.path.join(root, file)
                    with open(file_path, "r", encoding="utf-8-sig") as json_file:
                        try:
                            data = json.load(json_file)
                            # Check if expected keys exist
                            if 'dokumentstatus' in data and 'dokument' in data['dokumentstatus']:
                                dok_id = data['dokumentstatus']['dokument'].get('dok_id')
                                text = data['dokumentstatus']['dokument'].get('text')
                                # If dok_id or text is None, skip adding the document
                                if dok_id is not None and text is not None:
                                    document = RiksdagenDocument(id=dok_id, text=text)
                                    self.documents.append(document)
                                else:
                                    print(f"Skipping document {file_path}: Missing dok_id or text")
                            else:
                                print(f"Skipping document {file_path}: Missing 'dokumentstatus' or 'dokument'")
                        except json.JSONDecodeError as e:
                            print(f"Error loading JSON from {file_path}: {e}")

    def print_number_of_documents(self):
        # Print or use the variable containing all text
        print(f"number of documents: {len(self.documents)}")

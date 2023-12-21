# import string
# from typing import List
#
# from stop_words import get_stop_words
# import jsonlines
# from pandas import DataFrame
# from pydantic import BaseModel
# import pandas as pd
# import re
# import matplotlib.pyplot as plt
# from sklearn.feature_extraction.text import CountVectorizer
# from tqdm import tqdm
# from wordcloud import WordCloud
#
#
# class RiksdagenJsonFileProcessor(BaseModel):
#     """This model analyses and gives an
#     overview of the sentences in a given jsonl output file"""
#
#     file_path: str
#     additional_stop_words: List[str] = [
#         "ska",
#         "enligt",
#         "även",
#         "samt",
#         "finns",
#         "får",
#         "också",
#         "kap",
#         "vis",
#         "andra",
#         "genom",
#         "innebär",
#         "in",
#         "dock",
#         "rätt",
#         "ds",
#         "d",
#         "bör",
#     ]
#     cleaned_sentences_df: DataFrame = DataFrame()
#     stop_words: List[str] = []
#
#     class Config:
#         arbitrary_types_allowed = True
#
#     def process_json(self):
#         self.gather_and_print_basic_statistics()
#         self.create_dataframe_with_clean_sentences()
#         # self.generate_wordcloud()
#         # self.generate_document_term_matix()
#
#     def gather_and_print_basic_statistics(self):
#         print("Loading JSONL and calculating statistics")
#         total_lines = 0
#         total_suitable_lines = 0
#         total_tokens = 0
#         lang_stats = {}
#
#         with jsonlines.open(self.file_path) as reader:
#             for line in reader:
#                 total_lines += 1
#                 if line.get("suitable", False):
#                     total_suitable_lines += 1
#                     lang = line.get("lang", "")
#                     if lang:
#                         lang_stats[lang] = lang_stats.get(lang, 0) + 1
#                     tokens = line.get("tokens", 0)
#                     total_tokens += tokens
#
#         suitable_percentage = (
#             (total_suitable_lines / total_lines) * 100 if total_lines > 0 else 0
#         )
#
#         print(f"Total number of lines: {total_lines}")
#         print(f"Total number of suitable lines: {total_suitable_lines}")
#         print(f"Percentage suitable sentences: {round(suitable_percentage)}%")
#         print("Language statistics for suitable lines:")
#         for lang, count in lang_stats.items():
#             print(f"- {lang}: {count}")
#         if total_suitable_lines > 0:
#             print(
#                 f"Average number of tokens per suitable line: {total_tokens / total_suitable_lines}"
#             )
#         print(f"Total number of tokens: {total_tokens}")
#
#     def load_suitable_sentences(self) -> DataFrame:
#         suitable_sentences = []
#
#         with jsonlines.open(self.file_path) as reader:
#             for line in reader:
#                 if line.get("suitable", True):
#                     suitable_sentences.append(line.get("sentence", ""))
#
#         df = pd.DataFrame({"sentence": suitable_sentences})
#         return df
#
#     @staticmethod
#     def create_plot(name: str, df: DataFrame):
#         # Create a DataFrame of word frequencies
#         word_freq = df
#
#         # Sum the frequencies of each word
#         word_freq_sum = word_freq.sum()
#
#         # Sort words by frequency in descending order
#         word_freq_sum = word_freq_sum.sort_values(ascending=False)
#
#         # Plotting the top 10 most frequent words
#
#         top_words = word_freq_sum.head(10)
#         top_words.plot(kind="bar", figsize=(10, 6))
#         plt.title("Top 10 Most Frequent Words")
#         plt.xlabel("Words")
#         plt.ylabel("Frequency")
#         # fix for readability:
#         plt.xticks(
#             rotation=45, ha="right"
#         )  # Rotate x-axis labels for better readability
#         plt.tight_layout()
#         plt.subplots_adjust(bottom=0.3)  # Increase space below bars
#         # Save the plot to disk
#         plt.savefig(name)
#         # plt.show()
#
#     @staticmethod
#     def clean_text_round1(text):
#         """Make text lowercase, remove text in square brackets, remove punctuation and remove words containing numbers."""
#         text = text.lower()
#         text = re.sub("\[.*?\]", "", text)
#         text = re.sub("[%s]" % re.escape(string.punctuation), "", text)
#         text = re.sub("\w*\d\w*", "", text)
#         return text
#
#     def prepare_stopwords(self):
#         self.stop_words = get_stop_words("sv")
#         self.stop_words.extend(self.additional_stop_words)
#         # print(stop_words)
#
#     def create_dataframe_with_clean_sentences(self):
#         print("creating dataframe with clean and suitable sentences")
#         # Filter the DataFrame to get sentences where 'suitable' is True
#         suitable_sentences = self.load_suitable_sentences()
#
#         # Apply a first round of text cleaning techniques
#         round1 = lambda x: self.clean_text_round1(x)
#
#         # Let's take a look at the updated text
#         self.cleaned_sentences_df = pd.DataFrame(
#             suitable_sentences["sentence"].apply(round1)
#         )
#         print(self.cleaned_sentences_df)
#         # self.prepare_stopwords()
#         # self.calculate_trigram_dtm()
#         # exit()
#
#     # def calculate_trigram_dtm(self):
#     #     """This requires a lot of memory, which I don't have."""
#     #     # Create a trigram vectorizer
#     #     print("calculating trigrams")
#     #     # Assuming data_clean.sentence is your complete dataset
#     #
#     #     # Define the chunk size
#     #     chunk_size = 10000  # You can adjust this based on your memory constraints
#     #
#     #     # Get the total number of sentences
#     #     total_sentences = len(self.cleaned_sentences_df.sentence)
#     #
#     #     # Initialize an empty DataFrame to store the final result
#     #     data_dtm_trigram = pd.DataFrame()
#     #
#     #     # Process the data in chunks
#     #     for i in tqdm(range(0, total_sentences, chunk_size)):
#     #         # Get the chunk of sentences
#     #         chunk = self.cleaned_sentences_df.sentence[i:i + chunk_size]
#     #
#     #         # Create and fit the vectorizer for this chunk
#     #         trigram_vectorizer = CountVectorizer(ngram_range=(3, 3), stop_words=self.stop_words)
#     #         data_cv_trigram = trigram_vectorizer.fit_transform(chunk)
#     #
#     #         # Convert the chunk to a DataFrame
#     #         chunk_df = pd.DataFrame(
#     #             data_cv_trigram.toarray(),
#     #             columns=trigram_vectorizer.get_feature_names_out()
#     #         )
#     #
#     #         # Append the chunk DataFrame to the final result
#     #         data_dtm_trigram = pd.concat([data_dtm_trigram, chunk_df], axis=0)
#     #
#     #     # Reset the index of the final DataFrame
#     #     data_dtm_trigram.reset_index(drop=True, inplace=True)
#     #     # Now data_dtm_trigram contains the result of processing the data in smaller batches
#     #     print(data_dtm_trigram)
#
#     # trigram_vectorizer = CountVectorizer(ngram_range=(3, 3), stop_words=stop_words)
#     # data_cv_trigram = trigram_vectorizer.fit_transform(data_clean.sentence)
#     # data_dtm_trigram = pd.DataFrame(
#     #     data_cv_trigram.toarray(),
#     #     columns=trigram_vectorizer.get_feature_names_out(),
#     # )
#
#     # # Create a bigram vectorizer
#     # print("calculating bigrams")
#     # bigram_vectorizer = CountVectorizer(ngram_range=(2, 2), stop_words=stop_words)
#     # data_cv_bigram = bigram_vectorizer.fit_transform(data_clean.sentence)
#     # data_dtm_bigram = pd.DataFrame(
#     #     data_cv_bigram.toarray(), columns=bigram_vectorizer.get_feature_names_out()
#     # )
#     # # data_dtm_bigram.to_csv("bigram_document-term_matrix.csv")
#     #
#     # # monogram vectorizer
#     # print("calculating monograms")
#     # cv = CountVectorizer(stop_words=stop_words)
#     # data_cv = cv.fit_transform(data_clean.sentence)
#     # data_dtm = pd.DataFrame(data_cv.toarray(), columns=cv.get_feature_names_out())
#     # print(data_dtm)
#     # # data_dtm.to_csv("monogram_document-term_matrix.csv")
#     #
#     # # def create_plots(self):
#     # print("creating plots of most frequent words")
#     #
#     # self.create_plot(name="top_10_monogram.png", df=data_dtm)
#     # self.create_plot(name="top_10_bigram.png", df=data_dtm_bigram)
#     # self.create_plot(name="top_10_trigram.png", df=data_dtm_trigram)
#     #
#     def generate_wordcloud(self):
#         print("creating wordcloud")
#         from wordcloud import WordCloud
#
#         # Join the cleaned sentences into a single string
#         # We sample here to avoid out of memory errors
#         text = " ".join(self.cleaned_sentences_df.sample(100000).sentence)
#         text_without_stopwords = " ".join(
#             [word for word in text.split() if word.lower() not in self.stop_words]
#         )
#
#         wordcloud = WordCloud(width=800, height=400, background_color="white").generate(
#             text_without_stopwords
#         )
#
#         # Display the word cloud using matplotlib
#         plt.figure(figsize=(10, 6))
#         plt.imshow(wordcloud, interpolation="bilinear")
#         plt.axis("off")  # Turn off axis numbers
#         plt.title("Word Cloud of Most Frequent Words")
#         plt.savefig("wordcloud.png")

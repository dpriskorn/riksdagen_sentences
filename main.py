"""
algorithm

read all txt from the directory using the language code into a corpus
clean the corpus
use the LexSRT API to analyze the sentence

extract single words into dataframe
create document-term matrix
calculate frequency for all words
store each word, frequency pair in a database
show this information for each word in a sentence
"""
import hashlib
import os
import uuid

import matplotlib.pyplot as plt
from pandas import DataFrame

max_chunks = 10
additional_stop_words = [
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

# Replace 'path/to/your/directory' with the path to your directory containing the .txt files
language_directory = "data/sv"

# Variable to store the concatenated contents
all_text = ""

# Walk through all directories and subdirectories
for root, dirs, files in os.walk(language_directory):
    for file in files:
        if file.endswith(".txt"):
            file_path = os.path.join(root, file)
            with open(file_path, "r") as txt_file:
                # # Load the JSON data
                # data = json.loads(json_data)
                # Load the JSON data
                # data = json.loads(json_data)
                #
                # # Extract the value of 'dok_id'
                # dok_id = data['dokumentstatus']['dokument']['dok_id']
                file_contents = txt_file.read()
                all_text += file_contents + "\n"  # Add a newline between file contents

# Print or use the variable containing all text
# print(all_text)

# Remove newline characters using replace()
text_without_newlines = all_text.replace("\n\n", "\n")

print("text loading from files done")
# print(text_without_newlines)

import spacy
import pandas as pd


# Function to chunk the text
def chunk_text(text, chunk_size=100000):
    chunks = []
    start = 0
    while start < len(text):
        chunks.append(text[start : start + chunk_size])
        start += chunk_size
    return chunks


# Load the Swedish language model
nlp = spacy.load("sv_core_news_sm")

# Your corpus containing Swedish text
full_text = text_without_newlines

# Counting words in the text
word_count = len(full_text.split())

# Displaying the word count
print(f"Number of words before tokenization: {word_count}")

# Chunk the text
text_chunks = chunk_text(full_text)

# Count the number of chunks
num_chunks = len(text_chunks)

# Display the number of chunks
print(f"Number of chunks: {num_chunks}")

# Process each chunk separately
sentences = []
count = 1
for chunk in text_chunks:
    print(f"loading chunk {count}/{num_chunks}")
    doc = nlp(chunk)
    sentences.extend([sent.text for sent in doc.sents])
    if count == max_chunks:
        break
    count += 1

# Creating a DataFrame with a column 'sentences'
df = pd.DataFrame({"sentences": sentences})

# Generate UUIDs for each sentence and add them to a new 'uuid' column
df['uuid'] = [str(uuid.uuid4()) for _ in range(len(df))]

# Generate MD5 hash for each sentence and add them to a new 'md5_hash' column
def generate_md5_hash(sentence):
    return hashlib.md5(sentence.encode()).hexdigest()

df['md5_hash'] = df['sentences'].apply(generate_md5_hash)

# Remove newlines from the end of sentences in the 'sentences' column
df["sentences"] = df["sentences"].str.rstrip("\n")

# Display the DataFrame
print(df)
df.to_csv("departementserien.csv")
df.to_pickle("departementserien.pickle")
exit()


def suitable_sentence(sentence):
    # Split the sentence into words
    words = sentence.split()

    # Check if the sentence has more than 5 words
    if len(words) > 5:
        return True
    else:
        return False


# Apply the suitable_sentence function to the 'sentences' column
df["suitable"] = df["sentences"].apply(suitable_sentence)

# Select five random sentences
random_sentences = df["sentences"].sample(n=5, random_state=1)

# Test the function with example sentences
for idx, sentence in enumerate(random_sentences, 1):
    if suitable_sentence(sentence):
        print(f"Sentence {idx}: '{sentence}' is suitable.")
    else:
        print(f"Sentence {idx}: '{sentence}' is not suitable.")

print("creating doucment-term matrix of suitable sentences")
# We are going to create a document-term matrix using CountVectorizer, and exclude common English stop words
from sklearn.feature_extraction.text import CountVectorizer
from stop_words import get_stop_words

# print(stop_words)

stop_words = get_stop_words("sv")
stop_words.extend(additional_stop_words)
# Filter the DataFrame to get sentences where 'suitable' is True
suitable_sentences = df[df["suitable"]]["sentences"]

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
data_cv_trigram = trigram_vectorizer.fit_transform(data_clean.sentences)
data_dtm_trigram = pd.DataFrame(data_cv_trigram.toarray(), columns=trigram_vectorizer.get_feature_names_out())
#data_dtm_trigram.to_csv("trigram_document-term_matrix.csv")

# Create a bigram vectorizer
print("calculating bigrams")
bigram_vectorizer = CountVectorizer(ngram_range=(2, 2), stop_words=stop_words)
data_cv_bigram = bigram_vectorizer.fit_transform(data_clean.sentences)
data_dtm_bigram = pd.DataFrame(data_cv_bigram.toarray(), columns=bigram_vectorizer.get_feature_names_out())
#data_dtm_bigram.to_csv("bigram_document-term_matrix.csv")

# monogram vectorizer
print("calculating monograms")
cv = CountVectorizer(stop_words=stop_words)
data_cv = cv.fit_transform(data_clean.sentences)
data_dtm = pd.DataFrame(data_cv.toarray(), columns=cv.get_feature_names_out())
print(data_dtm)
#data_dtm.to_csv("monogram_document-term_matrix.csv")

print("creating plots of most frequent words")

def create_plot(name: string, df: DataFrame):
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

create_plot(name="top_10_monogram.png", df=data_dtm)
create_plot(name="top_10_bigram.png", df=data_dtm_bigram)
create_plot(name="top_10_trigram.png", df=data_dtm_trigram)

print("creating wordcloud")
from wordcloud import WordCloud

# Join the cleaned sentences into a single string
text = " ".join(data_clean.sentences)
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

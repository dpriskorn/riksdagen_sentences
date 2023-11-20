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

from models.riksdagen_analyzer import RiksdagenAnalyzer


ra = RiksdagenAnalyzer()
ra.start()
# print(ra.df)


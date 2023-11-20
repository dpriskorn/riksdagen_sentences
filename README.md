# riksdagen2sentences
Project to sentenize all the open data of Riksdagen to create an easily linkable 
dataset of sentences that can be refered to from Wikidata lexemes and other resources. 

The advantage of such a data set is huge from a language perspective. The sentences contain valuable information about what is going on in society. They contain a lot of new words which enter the language via political dialogue or written documents from the Swedish state institutions.

Keywords: NLP, data science, open data, swedish, open government data, riksdagen, sweden

# Idea
Use spaCy to create the first version.
All sentences are language detected, hashed using md5 and given an UUID which is unique for each release. 

As better sentenizing becomes available or Riksdagen improve their data over time, the hashes and UUIDs will change, but all released versions will be locked in time and can always be refered to consistently and reliably.

## Sources
https://www.riksdagen.se/sv/dokument-och-lagar/riksdagens-oppna-data/dokument/

## Inspiration
Alice Zhao https://www.youtube.com/watch?v=8Fw1nh8lR54
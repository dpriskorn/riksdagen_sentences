# riksdagen sentences
Project that aims to analyze and sentenize all the open 
data of Riksdagen using spaCy 
to create an easily linkable 
dataset of sentences that can be refered to from 
Wikidata lexemes and other resources. 

The advantage of such a dataset is huge from a language perspective. 
The sentences contain valuable information about what is going on in society. 
They contain a lot of new words which enter the language via political dialogue 
or written documents from institutions in the Swedish state .

Keywords: NLP, data science, open data, swedish, 
open government data, riksdagen, sweden, API

## Idea
Use spaCy to create the first version.
All sentences are language detected and given an 
UUID which is unique for each release. 

As better sentenizing becomes available or Riksdagen improve their 
data over time, the hashes and UUIDs will change, but all released 
versions will be locked in time and can always be refered to 
consistently and reliably.

## Features
* reliability
* locked in time
* referencable
* language detected
* uniquely identifiable
* linkable (the individual sentences are not planned to be 
linkable at this stage, but the release is and line numbers 
or UUIDs can be used to link with no ambiguity)

## Scope
This way of chopping up open data can be applied to any open data, provided that it is in a machine readable form like TEXT or HTML.

Riksdagen has about 600k documents that can be downloaded as open data.
The size of the resulting database has been estimated to >1TB when the analysis is complete.

This project is a stepping stone to a larger database of sentences and tokens that we can use to enrich the lexicographic data in Wikidata.

## Installation
Clone the repo

Run

`$ pip install poetry && poetry install`

Also download the model needed

`$ python -m spacy download sv_core_news_lg`
(250 MB)

## Use
Example
`$ python riksdagen_analyzer --analyze proposition`

## Sources
### Unilingual
* Riksdagen open data: 600k documents ~1TB database size in total https://www.riksdagen.se/sv/dokument-och-lagar/riksdagens-oppna-data/dokument/
### Multilingual
* Digital Corpus of the European Parliament https://wt-public.emm4u.eu/Resources/DCEP-2013/DCEP-Download-Page.html (EU languages)
* europarl corpus https://www.statmt.org/europarl/ (EU languages)
* wikisentences https://analytics.wikimedia.org/published/datasets/one-off/santhosh/wikisentences/ (all Wikipedia languages)
* The European Parliamentary Comparable and Parallel Corpora https://www.islrn.org/resources/036-939-425-010-1/ (en, es)
* Corrected & Structured Europarl Corpus https://pub.cl.uzh.ch/wiki/public/costep/start (EU languages)

## Inspiration
Alice Zhao https://www.youtube.com/watch?v=8Fw1nh8lR54

## License
GPLv3+

## What I learned
* the default sentenizer for Swedish in spaCy is not ideal
* fasttext langdetect cannot reliably detect language of sentences with only one token/word
* chatgpt can write good code, but it still outputs wonky code sometimes
* working on millions of sentences with NLP takes time even on a fast machine 
like my 8th gen 8-core i5 laptop
* langdetect is slow and only utilizes 1 CPU
* it's so nice to work with classes and small methods and 
combining them in ways that makes sense. KISS!

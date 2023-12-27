# Riksdagen Sentences
This civic science project aims to analyze and sentenize all the open 
data of Riksdagen using spaCy 
to create an easily linkable 
dataset of sentences that can be refered to from 
Wikidata lexemes and other resources. 

The advantage of such a dataset is huge from a language perspective. 
The sentences contain valuable information about what is going on in society. 
They contain a lot of words, phrases and idioms which are highly valuable to anyone interested in the language.
The 600k documents to be analyzed contains a lot of political dialogue and written documents from institutions in the Swedish state.

Keywords: NLP, data science, open data, swedish, 
open government data, riksdagen, sweden, API

# Author
[Dennis Priskorn](https://www.wikidata.org/wiki/Q111016131).

## Idea
Use spaCy to create the first version.
All sentences are language detected and given an 
UUID which is unique for each release. 

As better sentenizing becomes available or Riksdagen improve their 
data over time, the hashes and UUIDs will change, but all released 
versions will be locked in time and can always be refered to 
consistently and reliably.

The resulting dataset is planned to be released in Zenodo 
and is expected to be around 1TB  

## Features
* reliability
* locked in time
* referencable
* language detected (using Fasttext langdetect)
* uniquely identifiable
* linkable (the individual sentences are not planned to be 
linkable at this stage, but the release is and line numbers 
or UUIDs can be used to link with no ambiguity)
* Named Entity Recognition entities for each sentence and document
* APIs
  * /lookup endpoint to get sentences to use as usage examples for [Wikidata lexemes](https://www.wikidata.org/wiki/Wikidata:Lexicographical_data) (based on the needs of [Luthor](https://luthor.toolforge.org/))

## Scope
This way of chopping up open data can be applied to any open data, provided that it is in a machine readable form like TEXT, XML, JSON or HTML.

Riksdagen has about 600k documents that can be downloaded as open data.

This project is a stepping stone to an even larger database of sentences and tokens that we can use to enrich the lexicographic data in Wikidata.

## Statistics
See [STATISTICS.md](/STATISTICS.md)

## Design

### API design inspired by
* https://medium.com/@jccguimaraes/designing-an-api-6609eb771b18
* https://levelup.gitconnected.com/to-create-an-evolvable-api-stop-thinking-about-urls-2ad8b4cc208e
* https://en.wikipedia.org/wiki/Don%27t_Make_Me_Think
* https://jsonapi.org/format/#document-structure

### Data model
![Datamodel](/diagrams/datamodel.svg)

[UML source](/diagrams/datamodel.puml)

## Installation
Clone the repo

Run

`$ pip install poetry && poetry install`

Also download the model needed

`$ python -m spacy download sv_core_news_lg`
(250 MB)

Now download some of the source datasets from Riksdagen and put them in a data/sv/ folder hierarchy.

## Use
`$ python riksdagen_analyzer --analyze`

## Sources
### Mostly unilingual
* (sv) Riksdagen open data: ~600k machine readable HTML/TEXT documents ~1TB database size in total https://www.riksdagen.se/sv/dokument-och-lagar/riksdagens-oppna-data/dokument/
* (da) Folketinget open data: ~500k programmatically generated PDF documents https://www.ft.dk/da/dokumenter/aabne_data#276BF4DB3854444286D8F71F742FD018

## Related corpora
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
* chatgpt is very good at creating sql queries!
* working on millions of sentences with NLP takes time even on a fast machine 
like my 8th gen 8-core i5 laptop
* python langdetect was too slow and only utilized 1 CPU, swiching to fasttext langdetect was a bit challenging because I had to fix the python module
* it's so nice to work with classes and small methods and 
combining them in ways that makes sense. KISS!

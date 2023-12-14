# riksdagen2sentences
Project that aims to sentenize all the open data of Riksdagen to create an easily linkable 
dataset of sentences that can be refered to from Wikidata lexemes and other resources. 

The advantage of such a data set is huge from a language perspective. The sentences contain valuable information about what is going on in society. They contain a lot of new words which enter the language via political dialogue or written documents from the Swedish state institutions.

Keywords: NLP, data science, open data, swedish, open government data, riksdagen, sweden

## Idea
Use spaCy to create the first version.
All sentences are language detected, hashed using md5 and given an UUID which is unique for each release. 

As better sentenizing becomes available or Riksdagen improve their data over time, the hashes and UUIDs will change, but all released versions will be locked in time and can always be refered to consistently and reliably.

## Features
* reliability
* locked in time
* referencable
* language detected
* uniquely identifiable
* linkable (the individual sentences are not planned to be linkable at this stage, but the release is and line numbers 
or UUIDs can be used to link with no ambiguity)

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
https://www.riksdagen.se/sv/dokument-och-lagar/riksdagens-oppna-data/dokument/

## Inspiration
Alice Zhao https://www.youtube.com/watch?v=8Fw1nh8lR54

## License
GPLv3+

## What I learned
* pandas is super nice and fast
* the default sentenizer for Swedish in spaCy is not ideal
* chatgpt can write good code, but it still outputs wonky code sometimes
* through chatgpt I used the progress library tqdm 
for the first time and it is very nice :)
* working on millions of sentences with NLP takes time even on a fast machine 
like my 8th gen 8-core i5 laptop
* langdetect is slow and only utilizes 1 CPU
* it's so nice to work with classes and small methods and combining them in ways that makes sense. KISS!
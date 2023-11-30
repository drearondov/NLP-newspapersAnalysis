# Pipelines

The project is comprised by 6 pipelines, and in this document I'll explain how
each of them work.

The main library used throughout the project is `pandas` to make and transform the
`Dataframes`.

## Data Retrieval

Data was retrieved from 12 newspaper accounts from the main newspapers of Perú.
The selected newspapers are the following:

- El Comercio
- La República
- Trome
- Ojo
- Correo
- Expreso
- La Razón
- Gestión
- Perú 21
- El Búho
- En sus Trece
- El Peruano

This pipeline has two steps, the first steps retrieves the ID for each of the
Twitter accounts, and the second retrieves the tweets of the week for each
newspaper.

The main library used during this step is `requests`.

**Inputs:** ~~None~~

**Outputs:** `newspapers_id`, `raw_data`

## Cleaning and Preproccesing

The first major pipeline, that takes the raw `JSON` data received from the API, and
converts it to a Pandas `Dataframe`. The cleaning process follows 4 major transformations.

- *Removes Tweets that do not contribute to the narrative overall:*
  Tweets like, the picture with the daily headlines, daily caricatures,
  horoscope, etc.
- *Converts numbers to words:* NLP procesing focuses mainly on words, and numbers
  help us make sense of the scale of things happening (I actually learned this because
  at first I took out al numbers, and some of the tweets no longer made sense)
- Cleaning symbols, puncturation and emojis
- *Remove calls to action and common words:* Words such as "Watch the video now",
  or "Read more" do not contribute to the narrative and as such they were removed

**Inputs:** `raw_data`

**Outputs:** `clean_data`

## Feature Engireering

The hear of the process, feature engineering is a process from which we can
shape the data in order to obtain the different data inputs we need for the
rest of the pipelines such as EDA, Sentiment Analysis and Topic Modeling.

The process is divided into 2 stages:

### Make Corpus

Takes the clean data and returns a `Dataframe` with the original text, and the
corpus form or the tweet.

**Inputs:** `clean_data`

**Outputs:** `corpus`

### Make DTM

To make the DTM, the corpus needs to be tokenized and then counted in order to
form the matrix. So during the first first part, we take the corpus and make
an NLP object, and during the second part we count the occurrences of each word
and build the matrix.

For the NLP objects, the NLP library used is `spacy` as it has a spanish model
and allows for a single step tokenization, lemmatization and removal of stop-words.

**Inputs:** `corpus`

**Outputs:** `data_dtm`, `dtm`, `dtm_newspaper`

## EDA

After the inital EDA process, some of the process were needed for further
analysis or to buils Analysis tools, as such, this pipeline exist to provide
those structures. The pipeline contains two nodes.

### Unique words

The calculation of the number of unique words per newspaper per week

**Inputs:** `dtm_newspaper`

**Outputs:** `unique_words`

### Top 30 words

Calculates the Top 30 words used by each newspaper each week. This stage is
particualrly useful as a QA control tool.

**Inputs:** `dtm_newspaper`

**Outputs:** `top30_df`

## Sentiment Analysis

A Machine Learning pipeline with two outputs:

- The probability that a certain tweets has a `POSITIVE`, `NEUTRAL` or
  `NEGATIVE` sentiment.
- The probability that a tweets expreses certain emotion

The library used for this is [`PySentimiento`](https://github.com/pysentimiento/pysentimiento/tree/master)

**Inputs:** `corpus`, `emotion_analyzer`, `sentiment_analyzer`

**Outputs:** `corpus_sentiment-emotion`

## Topic Modeling

A Machine Learning pipeline that aims to determine the topics used
by each newspaper throughout the weeks and thus showcase the change in
the narrative, in a straight forward manner.

The libraries used in this pipeline are: `Gensim`, `scipy`

**Inputs:** `data_dtm`, `dtm`

**Outputs:** `corpus_topic`

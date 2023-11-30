Data
====

As data moves along the pipelines it’s easy to forget what fields are in
the different structures stored, and with that we can only guess which
structure we might need to implement a new feature or how we can
optimise the existing structres. This becomes specially important when
the structres are stored in binary type files that cannot be easily
viewed. As such, this file contains information about each of the
structures that are present in the poject.

.. note::
   The data directory can be found in the ``conf/base.catalog.yml`` file.

Newspapers ID (name: ``newspapers_id``)
---------------------------------------

This is a JSON file that contains the Tweeter ID of the different users
as well as their Twitter handle, which is the identifier used across the
project for the newspaper.

**Structure**

.. code:: json

   {
       "twitter handle": "twitter id"
   }

Newspapers Raw tweets (name: ``newspapers_raw_tweets``)
-------------------------------------------------------

A folder containing JSON files straight form Twitter’s API. Each file
corresponds to a week’s tweet from each newspaper.

**Naming convention:**
``{year}w{week number}_data_{twitter handle}.json``

**Structure**

.. code:: json

   {
       "data": [
           {
               "edit_history_tweet_ids": [
                   "ID list if tweet has been edited"
               ],
               "public_metrics": {
                   "retweet_count": "int: # retweets",
                   "reply_count": "int: # replies",
                   "like_count": "int: # likes",
                   "quote_count": "int: # quotes",
                   "bookmark_count": "int: # bookmarks",
                   "impression_count": "int: # of impressions"
               },
               "conversation_id": "int: ID of the conversation",
               "created_at": "datetime ISO: Time created",
               "text": "string: Tweet's text",
               "id": "int: Tweet's ID",
               "possibly_sensitive": "bool: Identifies a tweets as possibly sensitive or not"
           },
       ]
   }

Raw Data (name: ``raw_data``)
-----------------------------

Raw data compiled into ``Dataframes``, saved into ``Feather`` format,
one per week.

**Naming Convention:** ``data_raw-({year}, {week}).feather``

**Structure**

== ====================== ===================
#  Column                 Type
== ====================== ===================
0  index                  int64
1  edit_history_tweet_ids object
2  created_at             datetime64[ns, UTC]
3  id                     object
4  conversation_id        object
5  possibly_sensitive     bool
6  text                   object
7  retweet_count          int64
8  reply_count            int64
9  like_count             int64
10 quote_count            int64
11 bookmark_count         int64
12 impression_count       int64
13 referenced_tweets      object
14 newspaper              object
15 year                   UInt32
16 week                   UInt32
17 year_week              object
== ====================== ===================

Clean Data (name: ``clean_data``)
---------------------------------

``Dataframes`` after going through the ``cleaning_and_preprocessing``
pipeline, saved as ``Feather`` files.

**Naming Convention:** ``data_clean-({year}, {week}).feather``

**Structure**

== ====================== ===================
#  Column                 Dtype
== ====================== ===================
0  index                  int64
1  edit_history_tweet_ids object
2  created_at             datetime64[ns, UTC]
3  id                     object
4  conversation_id        object
5  possibly_sensitive     bool
6  text                   object
7  retweet_count          int64
8  reply_count            int64
9  like_count             int64
10 quote_count            int64
11 bookmark_count         int64
12 impression_count       int64
13 referenced_tweets      object
14 newspaper              object
15 year                   UInt32
16 week                   UInt32
17 year_week              object
18 mentions               object
19 hasthags               object
20 text_clean             object
== ====================== ===================

Corpus (name: ``corpus``)
-------------------------

``Dataframes`` after the first step in the ``feature_engineering``
pipeline. Contains only the original text and the cleaned corpus.

**Naming Convention:** ``corpus-({year}, {week}).feather``

**Structure**

= ========== ===================
# Column     Type
= ========== ===================
0 index      int64
1 id         object
2 created_at datetime64[ns, UTC]
3 newspaper  object
4 text       object
5 corpus     object
= ========== ===================

Data DTM (name: ``data_dtm``)
-----------------------------

``Dataframes`` wit columns containing the data necesary to perform NLP
analysis as well as build a ``Document-Term-Matrix``. The format
selected was ``pickle`` because the ``Dataframes`` contain objects that
cannot be serialized into a ``feather`` format.

**Naming Convention:** ``data_dtm-({year}, {week}).pkl``

**Structure**

= ========== ===================
# Column     Type
= ========== ===================
0 index      int64
1 id         object
2 created_at datetime64[ns, UTC]
3 newspaper  object
4 text       object
5 corpus     object
6 doc        object
7 token      object
8 lemma      object
= ========== ===================

DTM (name:``dtm``)
------------------

``Dataframes`` where the index contains the ``ID`` of each tweet and the
columns correspond to the words in each tweet. The format selected is
``Feather``, as the cell values are counts of the repetitions to the
word.

**Naming Convention:** ``dtm-({year}, {week}).feather``

**Structure**

== =========
id … words …
== =========
…  …
== =========

DTM Newspaper (name: ``dtm_newspaper``)
---------------------------------------

Dataset with ``Dataframes`` are stored in a folder, one per week. Each
``Dataframe`` has columns corresponding to the handles of each
newspaper, and the index corresponds to the words used in that week. The
values are the weekly count of each words.

**Naming Convention:** ``dtm_newspaper-({year}, {week}).feather``

**Structure**

==== ================================
word {@twitter-handle}-{year}\_{week}
==== ================================
…    …
==== ================================

Sentiment and Emotion analyzer
------------------------------

```PySentimiento`` <https://github.com/pysentimiento/pysentimiento>`__
models stored as pickle objects.

**Name:** ``sentiment_analyzer``

**Name:** ``emotion_analyzer``

Corpus Sentiment-Emotion (name: ``corpus_sentimen-emotion``)
------------------------------------------------------------

Collection of ``Dataframes`` after sentiment and emotion analysis. The
``Dataframes`` contain the probabilities of the corpus of being
``POSITIVE``, ``NEGATIVE`` or ``NEUTRAL`` as well as the different
emotions.

**Naming Convention:**
``corpus_sentiment_emotion-({year}, {week}).feather``

Corpus Topic (name: ``corpus_topic``)
-------------------------------------

Collection of ``Dataframes`` after Topic Modeling has been performed.

**Naming Convention:** ``corpus_topic-({year}, {week}).feather``

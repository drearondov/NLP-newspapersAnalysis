Newspapers Analysis Documentation
=================================

Welcome to the documentation for my NLP project. This project allow for
NLP analysis of Newspaper Headlines found in the tweets of 12 of the
main newspapers of Perú. The analysis is mainly on the aspects that can
allow us to see changes in the narrative, and how do the changes affect
the engagement of the users.

To organise the project I used ``Kedro``, and divided the process into 6
pipelines:

1. Data Retrieval
2. Cleaning and Preprocessing
3. EDA
4. Feature Engineering
5. Sentiment Analysis
6. Topic Modeling

In the Following figure you can see how the pipelines are organized and
how they move the data though each one of the nodes.

.. figure:: _static/nlp-newspaper-pipeline.svg
   :figwidth: 50%

   Data flow through the Kedro pipelines

.. toctree::
   :hidden:

   data.rst
   pipelines.rst
   api.rst

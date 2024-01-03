# NLP - Peruvian Newspapers Analysis

![Python Version](https://img.shields.io/badge/python-%3E=3.9-blue?style=for-the-badge&logo=python&logoColor=white)
[![Powered by Kedro](https://img.shields.io/badge/powered_by-kedro-ffc900?logo=kedro&style=for-the-badge)](https://kedro.org)
![Code style badge](https://img.shields.io/badge/style-black-black?style=for-the-badge)
[![GitHub deployments](https://img.shields.io/github/deployments/drearondov/NLP-newspapersAnalysis/github-pages?style=for-the-badge&label=Documentation)](https://drearondov.github.io/NLP-newspapersAnalysis/)

A full Data Science pipeline project about the analysis of newspapers headlines
of Perú.

## Objective

To show how the narrative changes over a period of time in local media, from both
independent and main news outlets in Perú.

### Specific Objectives

- Show asociations between different words over a period of time to see how the
  narrative changes around ceirtain topics
- Show how the media can control the narrative, by looking into the different
  reactions people have to the tweets

## Project overview

The project consists of 6 pipelines that go from retrieving the data to the
data structures that can be used for Data Analysis and Visualisation. Below is
a picture of the pipelines flow.

![pipeline flow for the project](docs/_static/nlp-newspaper-pipeline.svg)

> You can also check the sister project [nlp-newspapersDashboard](https://github.com/drearondov/nlp-newspapersDashboard)
> where I buil a dashboard with the data comming from this project.

### Pipelines

- Data Retrieval
- Cleaning and Preprocessing
- Feature Engineering
- EDA
- Sentiment & Emotion Analysis
- Topic Modeling

Full documentation about the project and the data warehouse can be found in
the [documentation](https://drearondov.github.io/NLP-newspapersAnalysis/). And
if you want to learn more abouth the building process you can read the
accompaning blog post series on [Nou de Data](noudedata.com).

## Code and Resources used

- **Python version:** `3.11.5`
- **Packages:** Kedro, Pandas, Numpy, Plotly, Requests, Gensim,
  Textblob, PySentimiento

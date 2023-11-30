# # Pipeline exploration

# In this notebook I'll be looking at the Data Catalog throughout the pipelines to search for possible errors

# ## Data Raw

# %%
data_raw_dataset = catalog.load("raw_data")
data_raw = data_raw_dataset["data_raw-(2023, 22).feather"]()

# ## Data Clean

# %%
data_clean_dataset = catalog.load("clean_data")
data_clean = data_clean_dataset["data_clean-(2023, 22).feather"]()


# %%
data_clean.groupby(by="newspaper")["created_at"].agg(["min", "max"])

# %%
for data_index in data_clean_list:
    data_clean = data_clean_dataset[data_index]()
    if len(data_clean["newspaper"].unique()) < 12:
        print(f"File: {data_index}")
        print(f"Newspapers: {data_clean['newspaper'].unique()}")

# ## Top 30

# %%
top30_dataset = catalog.load("top30_df")
top30 = top30_dataset["top30-(2023, 21).feather"]()

# ## DTM Newspaper

# %%
dtm_newspaper_dataset = catalog.load("dtm_newspaper")
dtm_newspaper = dtm_newspaper_dataset["dtm_newspaper-(2022, 29).feather"]()

# ## DTM

# %%
dtm_dataset = catalog.load("dtm")
dtm = dtm_dataset["dtm-(2022, 27).feather"]()

# ## DTM Newspaper

# %%
dtm_dataset = catalog.load("dtm_newspaper")
dtm = dtm_dataset["dtm_newspaper-(2022, 27).feather"]()

# ## Data DTM

# %%
data_dtm_dataset = catalog.load("data_dtm")
data_dtm = data_dtm_dataset["data_dtm-(2022, 27).pkl"]()

# ## Corpus

# %%
corpus_dataset = catalog.load("corpus")
corpus = corpus_dataset["corpus-(2022, 27).feather"]()

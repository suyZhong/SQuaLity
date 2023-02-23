#!/usr/bin/env python3

import sqlparse

import os
import sys
SCRIPDIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPDIR))
from src import analyzer
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

# os get dir name
print(SCRIPDIR)

results_df = pd.read_csv("output/psql_results.csv", na_filter=False )
logs_df = pd.read_csv("output/psql_logs.csv", na_filter=False)

print(results_df.info())
print(logs_df.info())

errors_df = results_df[results_df['IS_ERROR']]
print(errors_df.info())

error_messages = errors_df['ERROR_MSG'].tolist()

# convert each row in errors_messages to a tf-idf vector
vectorizer = TfidfVectorizer()
X = vectorizer.fit_transform(error_messages)

# cluster X and return the cluster labels
print(X)
# print(list(zip(X, error_messages)))
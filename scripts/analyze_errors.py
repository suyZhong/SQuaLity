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
from sklearn.cluster import KMeans

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

# use Kmeans to cluster X and return the cluster labels
kmeans = KMeans(n_clusters=6, random_state=0).fit(X)

groups = {}
for i, label in enumerate(kmeans.labels_):
    if label not in groups:
        groups[label] = []
    groups[label].append(error_messages[i])

for label, messages in groups.items():
    print("Group {}".format(label))
    for message in messages:
        if message:
            print(message)
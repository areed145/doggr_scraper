#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 10 08:14:08 2018

@author: areed145
"""

from datalab.context import Context
import datalab.storage as storage
import datalab.bigquery as bq
import pandas as pd

# Dataframe to write
simple_dataframe = pd.DataFrame(data=[{1,2,3},{4,5,6}],columns=['a','b','c'])

sample_bucket_name = Context.default().project_id + '-datalab-example'
sample_bucket_path = 'gs://' + sample_bucket_name
sample_bucket_object = sample_bucket_path + '/Hello.txt'
bigquery_dataset_name = 'TestDataSet'
bigquery_table_name = 'TestTable'

# Define storage bucket
sample_bucket = storage.Bucket(sample_bucket_name)

# Create storage bucket if it does not exist
if not sample_bucket.exists():
    sample_bucket.create()

# Define BigQuery dataset and table
dataset = bq.Dataset(bigquery_dataset_name)
table = bq.Table(bigquery_dataset_name + '.' + bigquery_table_name)

# Create BigQuery dataset
if not dataset.exists():
    dataset.create()

# Create or overwrite the existing table if it exists
table_schema = bq.Schema.from_dataframe(simple_dataframe)
table.create(schema = table_schema, overwrite = True)

# Write the DataFrame to GCS (Google Cloud Storage)
%storage write --variable simple_dataframe --object $sample_bucket_object

# Write the DataFrame to a BigQuery table
table.insert_data(simple_dataframe)
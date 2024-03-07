# Databricks notebook source
import requests as r
import gzip as g
import os
import json
from pyspark.sql.functions import col, filter
from pyspark.sql.types import Row

num_of_experiments_to_process = -1 # int(dbutils.widgets.get("num_of_experiments_to_process"))

pipe_path = '/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_2/'
cif_path = pipe_path + 'cif/'

etag = None
url = 'https://files.wwpdb.org/pub/pdb/data/structures/all/mmCIF/'

#new_experiments = [ row['experiment'] for row in spark.sql("select * from pdb_pipeline.pipeline_input where finished_ts is null").rdd.collect() ]
experiments_to_process = []


with open('/Workspace/Users/nikita.ivanov@quantori.com/input/green_fluorescent_protein_experiments_list.txt', 'r') as file:
    if num_of_experiments_to_process < 0:
        new_experiments = [line.rstrip() for line in file]
    else:
        new_experiments = [line.rstrip() for line in file][:num_of_experiments_to_process]

already_present = spark.sql(f'select left( right(input_file_name(), 9), 4) as experiment, etag from pdb_pipeline.raw_data').rdd.collectAsMap()

for experiment in new_experiments:
    # 1) check if this experiment needs an update or initial load
    url = 'https://files.wwpdb.org/pub/pdb/data/structures/all/mmCIF/' + experiment.lower() + '.cif.gz'
    resp = r.head(url)
    if not resp.status_code == 200:
        experiment.upper()
        url = 'https://files.wwpdb.org/pub/pdb/data/structures/all/mmCIF/' + experiment.lower() + '.cif.gz'
        resp = r.head(url)
    if not resp.status_code == 200:
        print('url', url, 'sc', resp.status_code)
        continue
    experiment.upper()
    etag = resp.headers['ETag']

    # continue if we have latest version already
    if experiment in already_present and already_present[experiment] == etag:
        experiments_to_process.append((experiment,etag))
        print(f'experiment found, {experiment}' )
        continue

    # 2) if we're here - experiment not present in latest version
    resp = r.get(url)
    file_content_str = g.decompress(resp.content)

    cif_file_path = cif_path + experiment + '.cif'
    with open(cif_file_path, 'w') as file:
        file.write(file_content_str.decode('utf-8'))
    experiments_to_process.append((experiment,etag))

dbutils.jobs.taskValues.set(key = "experimints", value = experiments_to_process)

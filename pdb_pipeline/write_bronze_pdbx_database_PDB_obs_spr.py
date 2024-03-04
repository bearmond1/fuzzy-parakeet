# Databricks notebook source
# MAGIC %sh
# MAGIC cd /Workspace/Repos/nikita.ivanov@quantori.com/fuzzy-parakeet/pdb_pipeline/
# MAGIC ls
# MAGIC /Workspace/Repos/nikita.ivanov@quantori.com/fuzzy-parakeet/pdb_pipeline/bronze_template.py

# COMMAND ----------

a = '/Workspace/Repos/nikita.ivanov@quantori.com/fuzzy-parakeet/pdb_pipeline/bronze_template'

spark.sparkContext.addPyFile('/Workspace/Repos/nikita.ivanov@quantori.com/fuzzy-parakeet/pdb_pipeline/bronze_template.py')
from bronze_template import write_bronze

category_name = 'pdbx_database_PDB_obs_spr'
list_of_fields = ['experiment',
                  'pdb_id' ,
                  'replace_pdb_id' ,
                  'date' ,
                  'details' ,
                  'id']

write_bronze(category_name = category_name, list_of_fields = list_of_fields, spark = spark)

# COMMAND ----------

# MAGIC %md
# MAGIC import json
# MAGIC import pandas as pd
# MAGIC from pyspark.sql.types import StringType
# MAGIC from pyspark.sql.functions import lit
# MAGIC import os
# MAGIC
# MAGIC category_name = 'pdbx_database_PDB_obs_spr'
# MAGIC
# MAGIC #experiments_to_process = dbutils.jobs.taskValues.get(taskKey = "load_cif_files", key = "experimints", debugValue = [('100D','etag'), ('1B9C','etag'), ('1BFP','etag'), ('1C4F','etag')] )
# MAGIC #print(experiments_to_process)
# MAGIC
# MAGIC experiments = spark.sql(f"select experiment from pdb_pipeline.current_run_categories where category = '{category_name}'  ").toPandas()
# MAGIC experiments_to_process = [ row['experiment'] for index,row in experiments.iterrows()]
# MAGIC
# MAGIC schema_carcass_path = '/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_1/PDB_dict/schema_carcass.json'
# MAGIC with open(schema_carcass_path, 'r') as file:
# MAGIC   schema_carcass = json.load(file)
# MAGIC
# MAGIC pipe_path = '/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_2/'
# MAGIC
# MAGIC experiments_for_sql = ''
# MAGIC
# MAGIC files = os.listdir(f"{pipe_path}bronze/{category_name}/")
# MAGIC
# MAGIC for experiment in experiments_to_process:
# MAGIC     if not f'{experiment}.json' in files:
# MAGIC         continue
# MAGIC     with open(f"{pipe_path}bronze/{category_name}/{experiment}.json", 'r') as file:
# MAGIC         category_json = json.load(file)
# MAGIC     first_key = list(category_json.keys())[0]
# MAGIC     if not str(type(category_json[first_key])) == "<class 'list'>":
# MAGIC         try:
# MAGIC             entity_df = pd.DataFrame( category_json, index=[0])
# MAGIC         except:
# MAGIC             print(type(category_json[first_key]).__str__ )
# MAGIC             display(category_json)
# MAGIC             assert 1 == 0
# MAGIC     else:
# MAGIC         try:
# MAGIC             entity_df = pd.DataFrame( category_json )
# MAGIC         except:
# MAGIC             print(type(category_json[first_key]).__str__ )
# MAGIC             display(category_json)
# MAGIC             assert 1 == 0
# MAGIC
# MAGIC     sparkDF=spark.createDataFrame(entity_df)
# MAGIC     for attribute in schema_carcass[category_name]:
# MAGIC         if not attribute in category_json:
# MAGIC             sparkDF = sparkDF.withColumn(attribute, lit(None).cast(StringType()) )
# MAGIC     sparkDF = sparkDF.withColumn('experiment', lit(experiment).cast(StringType()) )
# MAGIC     sparkDF = sparkDF.select(
# MAGIC         'experiment',
# MAGIC         'pdb_id' ,
# MAGIC         'replace_pdb_id' ,
# MAGIC         'date' ,
# MAGIC         'details' ,
# MAGIC         'id' )
# MAGIC     sparkDF.write.insertInto(f'pdb_pipeline.bronze_{category_name}', overwrite=False)
# MAGIC
# MAGIC     experiments_for_sql = f"{experiments_for_sql}'{experiment}',"
# MAGIC
# MAGIC experiments_for_sql = experiments_for_sql[:len(experiments_for_sql)-1]
# MAGIC for attribute in schema_carcass[category_name]:
# MAGIC     stmnt = f"update pdb_pipeline.bronze_{category_name} set {attribute} = Null where (experiment in ({experiments_for_sql}) and {attribute} = '?')"
# MAGIC     spark.sql(stmnt)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pdb_pipeline.bronze_pdbx_database_PDB_obs_spr limit 100;

# COMMAND ----------

# MAGIC %md
# MAGIC create or replace table pdb_pipeline.bronze_pdbx_database_PDB_obs_spr(
# MAGIC   experiment string,
# MAGIC   pdb_id string,
# MAGIC   replace_pdb_id string,
# MAGIC   date string,
# MAGIC   details string,
# MAGIC   id string
# MAGIC )
# MAGIC using delta;

# COMMAND ----------

# MAGIC %md
# MAGIC create table pdb_pipeline.silver_pdbx_database_PDB_obs_spr(
# MAGIC   experiment string not null,
# MAGIC   pdb_id string not null,
# MAGIC   replace_pdb_id string not null,
# MAGIC   date date not null,
# MAGIC   details string ,
# MAGIC   id string not null
# MAGIC )
# MAGIC using delta;

# COMMAND ----------

# MAGIC %md
# MAGIC create or replace table pdb_pipeline.rejected_pdbx_database_PDB_obs_spr(
# MAGIC   experiment string,
# MAGIC   pdb_id string,
# MAGIC   replace_pdb_id string,
# MAGIC   date string,
# MAGIC   details string,
# MAGIC   id string
# MAGIC )
# MAGIC using delta;

# Databricks notebook source
import json
import pandas as pd
from pyspark.sql.types import StringType
from pyspark.sql.functions import when, lit, col


pipe_path = '/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_2/'
schema_carcass_path = '/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_1/PDB_dict/schema_carcass.json'

category_name = 'entity' # dbutils.widgets.get("category")

# experiments which has current category
experiments = spark.sql(f"select experiment from pdb_pipeline.current_run_categories where category = '{category_name}'  ").toPandas()
experiments_to_process = [ row['experiment'] for index,row in experiments.iterrows()]
assert experiments_to_process != []
print(experiments_to_process)

with open(schema_carcass_path, 'r') as file:
  schema_carcass = json.load(file)

fields_list = list(schema_carcass[category_name].keys())
experiments_for_sql = ''
dataframes = []

for experiment in experiments_to_process:
    # get json for current catogory & experiment
    with open(f"{pipe_path}bronze/{category_name}/{experiment}.json", 'r') as file:
        category_json = json.load(file)
    first_key = list(category_json.keys())[0]
    
    # create pandas df with respect to possibility of key-value category
    if not isinstance(category_json[first_key], list): 
        try:
            category_df = pd.DataFrame( category_json, index=[0])
        except:
            print(type(category_json[first_key]).__str__ )
            display(category_json)
            continue
    else:
        try:
            category_df = pd.DataFrame( category_json )
        except:
            print(type(category_json[first_key]).__str__ )
            display(category_json)
            continue

    category_df.replace(to_replace  = '?', value = None, inplace = True)
    # add missing columns
    sparkDF=spark.createDataFrame(category_df)
    for attribute in schema_carcass[category_name]:
        if not attribute in category_json:
            sparkDF = sparkDF.withColumn(attribute, lit(None).cast(StringType()) )
    sparkDF = sparkDF.withColumn('experiment', lit(experiment).cast(StringType()) )
    sparkDF = sparkDF.select((['experiment'] + fields_list))
    #sparkDF.write.insertInto(f'pdb_pipeline.bronze_{category_name}', overwrite=False)
    #display(sparkDF)
    dataframes.append(sparkDF)
    experiments_for_sql = f"{experiments_for_sql}'{experiment}',"

assert dataframes != []

experiments_for_sql = experiments_for_sql[:len(experiments_for_sql)-1]

# assemble experiments into single df
bronze_category_dataframe = dataframes[0]
for i,df in enumerate(dataframes):
    if not i == 0:
        bronze_category_dataframe = bronze_category_dataframe.union(df)

display(bronze_category_dataframe)

spark.sql(f'delete from pdb_pipeline.bronze_{category_name} where (experiment in ({experiments_for_sql}))')
bronze_category_dataframe.write.insertInto(f'pdb_pipeline.bronze_{category_name}', overwrite=False)

#for attribute in schema_carcass[category_name]:
    # ideally it would be single statement
#    stmnt = f" update pdb_pipeline.bronze_{category_name} set {attribute} = Null where (experiment in ({experiments_for_sql}) and {attribute} = '?');"
#    update_stmnt = update_stmnt + stmnt


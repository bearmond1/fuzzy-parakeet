# Databricks notebook source
import json
import pandas as pd
from pyspark.sql.types import StringType
from pyspark.sql.functions import lit

category_name = 'entity'

experiments_to_process = dbutils.jobs.taskValues.get(taskKey = "load_cif_files", key = "experimints", debugValue = [('100D','etag'), ('1B9C','etag'), ('1BFP','etag'), ('1C4F','etag')] )
print(experiments_to_process)

schema_carcass_path = '/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_1/PDB_dict/schema_carcass.json'
with open(schema_carcass_path, 'r') as file:
  schema_carcass = json.load(file)

pipe_path = '/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_2/'

experiments_for_sql = ''

for (experiment,etag) in experiments_to_process:
    with open(f"{pipe_path}bronze/{category_name}/{experiment}.json", 'r') as file:
        entity_json = json.load(file)
    first_key = list(entity_json.keys())[0]
    if not str(type(entity_json[first_key])) == "<class 'list'>":
        try:
            entity_df = pd.DataFrame( entity_json, index=[0])
        except:
            print(type(entity_json[first_key]).__str__ )
            display(entity_json)
            assert 1 == 0
    else:
        try:
            entity_df = pd.DataFrame( entity_json )
        except:
            print(type(entity_json[first_key]).__str__ )
            display(entity_json)
            assert 1 == 0

    sparkDF=spark.createDataFrame(entity_df)
    for attribute in schema_carcass[category_name]:
        if not attribute in entity_json:
            sparkDF = sparkDF.withColumn(attribute, lit(None).cast(StringType()) )
    sparkDF = sparkDF.withColumn('experiment', lit(experiment).cast(StringType()) )
    sparkDF = sparkDF.select(
        'experiment',
        'id' ,
        'details' ,
        'formula_weight' ,
        'pdbx_description' ,
        'pdbx_ec' ,
        'pdbx_entities_per_biological_unit' ,
        'pdbx_formula_weight_exptl' ,
        'pdbx_formula_weight_exptl_method' ,
        'pdbx_fragment' ,
        'pdbx_modification' ,
        'pdbx_mutation' ,
        'pdbx_number_of_molecules' ,
        'pdbx_parent_entity_id' ,
        'pdbx_target_id' ,
        'src_method' ,
        'type'  )
    sparkDF.write.insertInto(f'pdb_pipeline.bronze_{category_name}', overwrite=False)

    experiments_for_sql = f"{experiments_for_sql}'{experiment}',"

experiments_for_sql = experiments_for_sql[:len(experiments_for_sql)-1]
for attribute in schema_carcass[category_name]:
    stmnt = f"update pdb_pipeline.bronze_{category_name} set {attribute} = Null where (experiment in ({experiments_for_sql}) and {attribute} = '?')"
    #print(stmnt)
    spark.sql(stmnt)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC

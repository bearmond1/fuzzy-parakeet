# Databricks notebook source
import json
from pdbecif.mmcif_tools import MMCIF2Dict
from copy import deepcopy
import pandas as pd
from pyspark.sql.types import StringType
from pyspark.sql.functions import lit

spark.sql('truncate table pdb_pipeline.current_run_categories')

experiments_to_process = dbutils.jobs.taskValues.get(taskKey = "load_cif_files", key = "experimints", debugValue = [('100D','etag')] )
print(experiments_to_process)
pipe_path = '/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_2/'
schema_carcass_path = '/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_1/PDB_dict/schema_carcass.json'
cif_path = pipe_path + 'cif/'

with open(schema_carcass_path, 'r') as file:
  schema_carcass = json.load(file)

for (experiment,etag) in experiments_to_process:
    spark.sql(f'insert into pdb_pipeline.pipeline_input (experiment) values ("{experiment}")')
    cif_file_path = cif_path + experiment + '.cif'
    experiment_dict = MMCIF2Dict().parse(cif_file_path)
    experiment_name = list(experiment_dict.keys())[0]
    output_dict = {'data':experiment_dict[experiment_name], 'etag':etag}

    with open(pipe_path + 'raw_data/' + experiment + '.json', 'w') as file:
        json.dump(output_dict, file)

    # trim underscore
    new_file_uniformed = {}
    for category_name, category in experiment_dict[experiment_name].items():
        if category_name[0] == '_':
            category_name_normalized = category_name[1:]
        else:
            category_name_normalized = category_name

        new_file_uniformed[category_name_normalized] = category
    
    new_file_refined = deepcopy(new_file_uniformed)
    # remove data which is not in target schema
    for category in new_file_uniformed:
        if not category in schema_carcass:
            new_file_refined.pop(category)
        else:
            for attribute in new_file_uniformed[category]:
                if not attribute in schema_carcass[category]:
                    new_file_refined[category].pop(attribute)

    for category in new_file_refined:
        spark.sql(f"insert into pdb_pipeline.current_run_categories values ('{category}','{experiment}')")
        with open(f'{pipe_path}bronze/{category}/{experiment}.json', 'w') as file:
            json.dump(new_file_refined[category], file)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC     entity_dict = new_file_refined['entity']
# MAGIC     first_key = list(entity_dict.keys())[0]
# MAGIC     if not str(type(entity_dict[first_key])) == "<class 'list'>":
# MAGIC         try:
# MAGIC             entity_df = pd.DataFrame( new_file_refined['entity'], index=[0])
# MAGIC         except:
# MAGIC             print(type(entity_dict[first_key]).__str__ )
# MAGIC             display(new_file_refined['entity'])
# MAGIC             assert 1 == 0
# MAGIC     else:
# MAGIC         try:
# MAGIC             entity_df = pd.DataFrame( new_file_refined['entity'] )
# MAGIC         except:
# MAGIC             print(type(entity_dict[first_key]).__str__ )
# MAGIC             display(new_file_refined['entity'])
# MAGIC             assert 1 == 0
# MAGIC
# MAGIC     sparkDF=spark.createDataFrame(entity_df)
# MAGIC     for attribute in schema_carcass['entity']:
# MAGIC         if not attribute in new_file_refined['entity']:
# MAGIC             sparkDF = sparkDF.withColumn(attribute, lit(None).cast(StringType()) )
# MAGIC     sparkDF = sparkDF.withColumn('experiment', lit(experiment).cast(StringType()) )
# MAGIC     sparkDF = sparkDF.select(
# MAGIC         'experiment',
# MAGIC         'id' ,
# MAGIC         'details' ,
# MAGIC         'formula_weight' ,
# MAGIC         'pdbx_description' ,
# MAGIC         'pdbx_ec' ,
# MAGIC         'pdbx_entities_per_biological_unit' ,
# MAGIC         'pdbx_formula_weight_exptl' ,
# MAGIC         'pdbx_formula_weight_exptl_method' ,
# MAGIC         'pdbx_fragment' ,
# MAGIC         'pdbx_modification' ,
# MAGIC         'pdbx_mutation' ,
# MAGIC         'pdbx_number_of_molecules' ,
# MAGIC         'pdbx_parent_entity_id' ,
# MAGIC         'pdbx_target_id' ,
# MAGIC         'src_method' ,
# MAGIC         'type'  )
# MAGIC     sparkDF.write.insertInto('pdb_pipeline.bronze_entity', overwrite=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pdb_pipeline.bronze_entity;
# MAGIC --truncate table pdb_pipeline.bronze_entity;

import json
import pandas as pd
from pyspark.sql.types import StringType
from pyspark.sql.functions import lit
#from databricks.sdk.runtime import spark

def write_bronze(category_name, list_of_fields, spark):
  
    experiments = spark.sql(f"select experiment from pdb_pipeline.current_run_categories where category = '{category_name}'  ").toPandas()
    experiments_to_process = [ row['experiment'] for index,row in experiments.iterrows()]

    schema_carcass_path = '/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_1/PDB_dict/schema_carcass.json'
    with open(schema_carcass_path, 'r') as file:
        schema_carcass = json.load(file)

    pipe_path = '/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_2/'

    experiments_for_sql = ''

    for experiment in experiments_to_process:
        with open(f"{pipe_path}bronze/{category_name}/{experiment}.json", 'r') as file:
            catogory_json = json.load(file)
        first_key = list(catogory_json.keys())[0]
        if not str(type(catogory_json[first_key])) == "<class 'list'>":
            try:
                catogory_df = pd.DataFrame( catogory_json, index=[0])
            except:
                print(type(catogory_json[first_key]).__str__ )
                display(catogory_json)
                assert 1 == 0
        else:
            try:
                catogory_df = pd.DataFrame( catogory_json )
            except:
                print(type(catogory_json[first_key]).__str__ )
                display(catogory_json)
                assert 1 == 0

        sparkDF=spark.createDataFrame(catogory_df)
        for attribute in schema_carcass[category_name]:
            if not attribute in catogory_json:
                sparkDF = sparkDF.withColumn(attribute, lit(None).cast(StringType()) )
        sparkDF = sparkDF.withColumn('experiment', lit(experiment).cast(StringType()) )
        sparkDF = sparkDF.select(list_of_fields)
        sparkDF.write.insertInto(f'pdb_pipeline.bronze_{category_name}', overwrite=False)

        experiments_for_sql = f"{experiments_for_sql}'{experiment}',"

    experiments_for_sql = experiments_for_sql[:len(experiments_for_sql)-1]
    for attribute in schema_carcass[category_name]:
        stmnt = f"update pdb_pipeline.bronze_{category_name} set {attribute} = Null where (experiment in ({experiments_for_sql}) and {attribute} = '?')"
        #print(stmnt)
        spark.sql(stmnt)

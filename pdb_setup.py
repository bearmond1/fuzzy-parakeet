# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline logic
# MAGIC 1. take experiment from input table
# MAGIC 2. check which experiments needs update/initial process
# MAGIC 3. проверяем есть ли у нас актуальная схема ( отсортировать словарь указанной схемы, в строку, хэш записать как версию )
# MAGIC Актуализируем при необходимости
# MAGIC 4. параллельно с актуализацией словаря грузим файлы 
# MAGIC 5. файлы перегоняем в json
# MAGIC 6. со словарем и файлами в json приводим файлы к заданной схеме
# MAGIC 7. проверки на обязательные поля, лог отбракованных данных
# MAGIC 8. проверки на типы и загоняем данные в Сильвер слой, лог отбракованных данных

# COMMAND ----------


entities_to_process = spark.sql("select experiment from pdb_pipeline.current_run_categories  ").toPandas()
entities = [ row['experiment'] for index,row in entities_to_process.iterrows()]
print(entities)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table if not exists pdb_pipeline.current_run_categories (
# MAGIC   category string,
# MAGIC   experiment string
# MAGIC )
# MAGIC using delta;
# MAGIC
# MAGIC insert into pdb_pipeline.current_run_categories values ('category', 'experiment'), ('category1', 'experiment1');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pdb_pipeline.bronze_entity limit 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table pdb_pipeline.raw_data 
# MAGIC using json
# MAGIC location 'file:/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_2/raw_data';

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists raw_data;
# MAGIC create table raw_data 
# MAGIC using json
# MAGIC location 'file:/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_1/raw_data';
# MAGIC
# MAGIC select * from raw_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC select data._entity from raw_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC   left( right(input_file_name(), 9), 4) as experiment,
# MAGIC   etag,
# MAGIC   data
# MAGIC from raw_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table if not exists pipeline_input (experiment string, added_ts timestamp, finished_ts timestamp);
# MAGIC --insert into pipeline_input (experiment, added_ts) values ('100D', current_timestamp()), ('4HHB', current_timestamp());

# COMMAND ----------

# MAGIC %md
# MAGIC ###Download and write experiment as json , no tranformations, only version added

# COMMAND ----------

import requests as r
import gzip as g
from pdbecif.mmcif_tools import MMCIF2Dict

experiment_name = '100d'


resp = r.get('https://files.wwpdb.org/pub/pdb/data/structures/all/mmCIF/' + experiment_name + '.cif.gz')
experiment_name.upper()
print(resp.status_code)
file_content_str = g.decompress(resp.content)
etag = resp.headers['ETag']


import os
import json

pipe_path = '/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_1/'
cif_path = pipe_path + 'cif/'
cif_file_path = cif_path + experiment_name + '.cif'

with open(cif_file_path, 'w') as file:
    file.write(file_content_str.decode("utf-8"))

experiment = MMCIF2Dict().parse(cif_file_path)
experiment_name = list(experiment.keys())[0]
#print(experiment[experiment_name]['_entity'])
output_dict = {'data':experiment[experiment_name], 'etag':etag}

with open(pipe_path + 'raw_data/' + experiment_name + '.json', 'w') as fp:
    json.dump(output_dict, fp)

# COMMAND ----------

# MAGIC %md
# MAGIC #Lets try to get dict data from website...

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists required_categories_and_attributes (category string, attribute string);
# MAGIC insert into required_categories_and_attributes values 
# MAGIC   ('entity', '*'), 
# MAGIC   ('pdbx_database_PDB_obs_spr', '*'),
# MAGIC   ('entity_poly_seq', '*'),
# MAGIC   ('chem_comp', '*'),
# MAGIC   ('exptl', 'entry_id'),
# MAGIC   ('exptl', 'method');

# COMMAND ----------

# MAGIC %md
# MAGIC ##1. Get schema

# COMMAND ----------

import bs4 
import pprint 
from pprint import pprint
import requests as r
import json

required_full_categories = [ row['category'] for row in spark.sql("select category from required_categories_and_attributes where attribute = '*' ").rdd.collect()]

categories_site = 'https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Categories/'
items_site = 'https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_'
link_prefix = 'https://mmcif.wwpdb.org'

schema_carcass = {}
categories_dict = {}

for category in required_full_categories:
    category_attributes = []
    category_attributes_dict = {}
    page = r.get(categories_site + category + '.html')
    bs = bs4.BeautifulSoup(page.text, 'lxml')

    attrs = bs.find_all('ul', class_ = 'list-group')
    for attr in attrs:
        lis = attr.find_all('li', class_ = 'list-group-item')
        for li in lis:
            hrefs = li.findAll('a')
            for href in hrefs:
                try:
                    if 'mytip' in href.__dict__['attrs']['class']:
                        continue
                except:
                    pass
                href_itself = href.__dict__['attrs']['href']
                href_text = href.getText()
                #category_attributes.append((link_prefix + href_itself,href_text))
                category_attributes.append({'attribute_name': href_text.split('.')[1], 'link':link_prefix + href_itself })
                category_attributes_dict[href_text.split('.')[1]] = []
    schema_carcass[category] = category_attributes_dict
    categories_dict[category] = category_attributes

pprint(schema_carcass)
specified_attributes = [ (row['category'], row['attribute']) for row in spark.sql("select * from required_categories_and_attributes where not attribute = '*' ").rdd.collect()]

for (specified_category, specified_attribute) in specified_attributes:
    cat_dot_attr = '_' + specified_category + '.' + specified_attribute
    categories_dict[specified_category] = {'attribute_name': specified_attribute, 'link':items_site + cat_dot_attr + '.html' }

    if specified_category in schema_carcass:
        schema_carcass[specified_category].update({specified_attribute:[]})
    else:
        schema_carcass[specified_category] = {specified_attribute:[]}

#pprint(categories_dict)

with open('/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_1/PDB_dict/full_schema_dict.json', 'w') as file:
    json.dump(categories_dict, file)

with open('/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_1/PDB_dict/schema_carcass.json', 'w') as file:
    json.dump(schema_carcass, file)


# COMMAND ----------

# MAGIC %md
# MAGIC ##2. Merge existing data into defined schema

# COMMAND ----------

# MAGIC %md
# MAGIC ###2.1 TODO get only new files
# MAGIC looks like it doesn't like the path

# COMMAND ----------

df = spark.readStream.format('cloudFiles') \
    .option('cloudFiles.format', 'json') \
    .option('cloudFiles.schemaLocation', '/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_1/raw_data_stream_schema') \
    .option('cloudFiles.inferColumnTypes', 'true') \
    .load('file:/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_1/raw_data')

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###2.2 Merge file schema
# MAGIC we need to actually create table for each category from configuration
# MAGIC so, dynamic table creation from python
# MAGIC
# MAGIC or maybe put it together for the first iteration by hand ?

# COMMAND ----------

from pprint import pprint
import json
import os
from copy import deepcopy

# this will be auto loader result
new_files = ['100D.json']

pipeline_path = '/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_1/'
new_files_path = pipeline_path + 'raw_data/'

with open(pipeline_path + 'PDB_dict/full_schema_dict.json', 'r') as file:
  full_schema = json.load(file)

with open(pipeline_path + 'PDB_dict/schema_carcass.json', 'r') as file:
  schema_carcass = json.load(file)

folders = os.listdir(pipeline_path + 'bronze')

# create missing folders
for category in full_schema:
  if not category in folders:
    os.mkdir(pipeline_path + 'bronze/' + category)


for new_file_name in new_files:  
  file_path = new_files_path + new_file_name
  with open(file_path, 'r') as file:
    new_file = json.load(file)

  # key-value pairs -> keys list
  # also trim underscore in the beginnig of category name
  new_file_uniformed = {}
  for category_name, category in new_file['data'].items():
      new_category = {}
      for attribute_name, attribute in category.items():
          if type(attribute).__name__ == 'str':
            new_category[attribute_name] = [attribute]
          elif type(attribute).__name__ == 'list':
            new_category[attribute_name] = attribute
          else:
            print('wtf?', type(attribute))

      if category_name[0] == '_':
        category_name_normalized = category_name[1:]
      else:
        category_name_normalized = category_name

      new_file_uniformed[category_name_normalized] = new_category


  new_file_refined = deepcopy(new_file_uniformed)
  # remove data which is not in target schema
  for category in new_file_uniformed:
    if not category in schema_carcass:
      #print('category missing ',category)
      new_file_refined.pop(category)
    else:
      for attribute in new_file_uniformed[category]:
        if not attribute in schema_carcass[category]:
          new_file_refined[category].pop(attribute)
  

  # enrich files with missing categories
  schema_enforced_file = schema_carcass | new_file_refined
  # fill existing categories with missing attributes
  for category in schema_carcass:
    schema_enforced_file[category] = schema_carcass[category] | schema_enforced_file[category]
  
  # write 
  for category_name, category in schema_enforced_file.items():
    with open(pipeline_path + 'bronze/' + category_name + '/' + new_file_name, 'w') as file:
      json.dump(schema_enforced_file[category_name],file)


# COMMAND ----------

# MAGIC %md
# MAGIC ### We have written our file with proper schema, but there is arrays in columns, and we have to make it a table

# COMMAND ----------

import pandas as pd

df = pd.DataFrame( {'id': ['1', '2', '3'], 'type': ['polymer', 'non-polymer', 'water'], 'src_method': ['syn', 'syn', 'nat'], 'pdbx_description': ["DNA/RNA (5'-R(*CP*)-D(*CP*GP*GP*CP*GP*CP*CP*GP*)-R(*G)-3')", 'SPERMINE', 'water'], 'formula_weight': ['3078.980', '202.340', '18.015'], 'pdbx_number_of_molecules': ['2', '1', '67'], 'pdbx_ec': ['?', '?', '?'], 'pdbx_mutation': ['?', '?', '?'], 'pdbx_fragment': ['?', '?', '?'], 'details': ['?', '?', '?']})
display(df)

#Create PySpark DataFrame from Pandas
sparkDF=spark.createDataFrame(df) 
sparkDF.printSchema()
sparkDF.show()



# COMMAND ----------

df = spark.sql("select * from json.`file:/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_1/bronze/entity` ")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists bronze_entity;
# MAGIC create table bronze_entity
# MAGIC using json
# MAGIC location 'file:/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_1/bronze/entity';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_entity;

# COMMAND ----------

# MAGIC %md
# MAGIC # Futile attempt to parse PDB dictionary to get data schema 

# COMMAND ----------

#resp_dict = r.get('https://mmcif.wwpdb.org/dictionaries/ascii/mmcif_pdbx_v50.dic')
#PDBx_dict_text = resp.text
#pdbx_dict_path = pipe_path + 'PDBxDic.dic'

#with open(pdbx_dict_path, 'w') as file:
#    file.write(PDBx_dict_text)

from pdbecif.mmcif_tools import MMCIF2Dict
pdbx_dict = MMCIF2Dict().parse('/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_1/mmcif_pdbx_v50.dic')

first_key = list(pdbx_dict.keys())[0]
print(pdbx_dict[first_key])
pdbx_dict = pdbx_dict[first_key]

transposed = []
max_length = 0

for i in pdbx_dict.keys():
    if len(pdbx_dict[i]) > max_length:
        print(i,len(pdbx_dict[i]))
    max_length = max(max_length, len(pdbx_dict[i]))

print('max_length',max_length)
#max_length = 50

for i in range(max_length):
    newline = {}
    for key in pdbx_dict.keys():
        if key in ['_dictionary_history.revision','_dictionary_history.update','_dictionary_history.version']:
            continue
        if str(type(pdbx_dict[key])) == '<class \'dict\'>':
            continue
        print(key)
        if len(pdbx_dict[key]) < max_length:
            if len(pdbx_dict[key]) == 0:
                pass
                #value_from_array = pdbx_dict[key]
            else:
                print(key)
                value_from_array = pdbx_dict[key][0]
        else:
            try:
                value_from_array = pdbx_dict[key][i]
            except:
                print(type(pdbx_dict[key]))
                print(pdbx_dict[key])

        newline.update({key:value_from_array})
    transposed.append(newline)
    #with open('/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_1/PDB_dict/mmcif_pdbx_v50_line_' + str(i) + '.json', 'w') as fp:
    #    json.dump(newline, fp)



with open('/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_1/mmcif_pdbx_v50.json', 'w') as fp:
    json.dump(transposed, fp)


# COMMAND ----------

import Bio.PDB as bp
import json
from Bio.PDB import MMCIF2Dict as mmcif2dict

pdbx_dict = mmcif2dict.MMCIF2Dict('/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_1/mmcif_pdbx_v50.dic')
#mmcif_dict = bp.MMCIF2Dict('/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_1/mmcif_pdbx_v50.dic')

#print(pdbx_dict.keys())

transposed = []
max_length = 0

for i in pdbx_dict.keys():
    if len(pdbx_dict[i]) > max_length:
        print(i,len(pdbx_dict[i]))
    max_length = max(max_length, len(pdbx_dict[i]))

print(max_length)
max_length = 50

for i in range(max_length):
    newline = {}
    for key in pdbx_dict.keys():
        if key in ['_dictionary_history.revision','_dictionary_history.update','_dictionary_history.version']:
            continue
        if len(pdbx_dict[key]) < max_length:
            if len(pdbx_dict[key]) == 0:
                pass
                #value_from_array = pdbx_dict[key]
            else:
                value_from_array = pdbx_dict[key][0]
        else:
            value_from_array = pdbx_dict[key][i]
        newline.update({key:value_from_array})
    #transposed.append(newline)
    with open('/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_1/PDB_dict/mmcif_pdbx_v50_line_' + str(i) + '.json', 'w') as fp:
        json.dump(newline, fp)

    #a = pdbx_dict[i]
    #if i in ['_dictionary_history.revision','_dictionary_history.update','_dictionary_history.version']:
    #    continue
    #print(i,a)


#with open('/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_1/mmcif_pdbx_v50.json', 'w') as fp:
    #json.dump(transposed, fp)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from json.`file:/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_1/PDB_dict/`

# COMMAND ----------

# MAGIC %md
# MAGIC #Old code, we want to enrich and validate data from dictionary

# COMMAND ----------

from pdbecif.mmcif_tools import MMCIF2Dict
import json

full_entity = {  
  'experiment': [] ,
  'id': [] ,
  'details' : [] ,
  'formula_weight' : [] ,
  'pdbx_description' : [] ,
  'pdbx_ec' : [] ,
  'pdbx_entities_per_biological_unit' : [] ,
  'pdbx_formula_weight_exptl': [] ,
  'pdbx_formula_weight_exptl_method' : [] ,
  'pdbx_fragment' : [] ,
  'pdbx_modification' : [] ,
  'pdbx_mutation': [] ,
  'pdbx_number_of_molecules' : [] ,
  'pdbx_parent_entity_id': [] ,
  'pdbx_target_id' : [] ,
  'src_method' : [] ,
  'type': [] 
  }

our_keys = ['_entity','_pdbx_database_PDB_obs_spr','_entity_poly_seq','_chem_comp','_exptl.entry_id', '_exptl.method']
mmcif_dict = MMCIF2Dict()
cif_dict = mmcif_dict.parse(cwd + '/test.cif')

pdb_id = list(cif_dict.keys())[0]
print('pdb_id',pdb_id)

our_stuff = {}

for key in list(cif_dict[experiment_name].keys()):
    if key in our_keys:
        #print(key, ' found')
        category = cif_dict[experiment_name][key]
        if key == '_entity':
            category = full_entity | category
            print(category)
        our_stuff.update({key:category})

print('our_stuff',our_stuff)
our_stuff.update({'etag':etag, 'experiment_id':list(cif_dict.keys())[0]})

with open('/Workspace/Users/nikita.ivanov@quantori.com' + '/pdbs/raw_data/' + experiment_name + '.json', 'w') as fp:
    json.dump(our_stuff, fp)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists raw_pdb;
# MAGIC --SELECT * FROM json.`file:/Workspace/Users/nikita.ivanov@quantori.com/pdbs/test.json`;
# MAGIC
# MAGIC create table raw_pdb 
# MAGIC USING json
# MAGIC location 'file:/Workspace/Users/nikita.ivanov@quantori.com/pdbs/raw_data';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * FROM raw_pdb;
# MAGIC --describe table raw_pdb;
# MAGIC --select _entity, experiment_id from raw_pdb;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table pdb_pipeline.bronze_entity
# MAGIC (
# MAGIC   experiment string,
# MAGIC   id string,
# MAGIC   details string,
# MAGIC   formula_weight string,
# MAGIC   pdbx_description string,
# MAGIC   pdbx_ec string,
# MAGIC   pdbx_entities_per_biological_unit string,
# MAGIC   pdbx_formula_weight_exptl string,
# MAGIC   pdbx_formula_weight_exptl_method string,
# MAGIC   pdbx_fragment string,
# MAGIC   pdbx_modification string,
# MAGIC   pdbx_mutation string,
# MAGIC   pdbx_number_of_molecules string,
# MAGIC   pdbx_parent_entity_id string,
# MAGIC   pdbx_target_id string,
# MAGIC   src_method string,
# MAGIC   type string
# MAGIC )
# MAGIC using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pdb_pipeline.bronze_entity limit 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table bronze_entity;
# MAGIC insert into bronze_entity by name
# MAGIC select * except(rn)
# MAGIC from (
# MAGIC   select
# MAGIC   row_number() over (order by id) rn,
# MAGIC   experiment_id as experiment,
# MAGIC   explode(_entity.id) as id,
# MAGIC   _entity.details[rn-1] as details,
# MAGIC   _entity.formula_weight[rn-1] as formula_weight,
# MAGIC   _entity.pdbx_description[rn-1] as pdbx_description,
# MAGIC   _entity.pdbx_ec[rn-1] as pdbx_ec,
# MAGIC   _entity.pdbx_entities_per_biological_unit[rn-1] as pdbx_entities_per_biological_unit,
# MAGIC   _entity.pdbx_formula_weight_exptl[rn-1] as pdbx_formula_weight_exptl,
# MAGIC   _entity.pdbx_formula_weight_exptl_method[rn-1] as pdbx_formula_weight_exptl_method,
# MAGIC   _entity.pdbx_fragment[rn-1] as pdbx_fragment,
# MAGIC   _entity.pdbx_modification[rn-1] as pdbx_modification,
# MAGIC   _entity.pdbx_mutation[rn-1] as pdbx_mutation,
# MAGIC   _entity.pdbx_number_of_molecules[rn-1] as pdbx_number_of_molecules,
# MAGIC   _entity.pdbx_parent_entity_id[rn-1] as pdbx_parent_entity_id,
# MAGIC   _entity.pdbx_target_id[rn-1] as pdbx_target_id,
# MAGIC   _entity.src_method[rn-1] as src_method,
# MAGIC   _entity.type[rn-1] as type
# MAGIC   FROM (select _entity, experiment_id from raw_pdb)
# MAGIC ) 
# MAGIC ;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from bronze_entity;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists silver_entity;
# MAGIC create table silver_entity
# MAGIC (
# MAGIC   experiment string,
# MAGIC   id string,
# MAGIC   details string,
# MAGIC   formula_weight float,
# MAGIC   pdbx_description string,
# MAGIC   pdbx_ec string,
# MAGIC   pdbx_entities_per_biological_unit float,
# MAGIC   pdbx_formula_weight_exptl float,
# MAGIC   pdbx_formula_weight_exptl_method string,
# MAGIC   pdbx_fragment string,
# MAGIC   pdbx_modification string,
# MAGIC   pdbx_mutation string,
# MAGIC   pdbx_number_of_molecules int,
# MAGIC   pdbx_parent_entity_id string,
# MAGIC   pdbx_target_id string,
# MAGIC   src_method string,
# MAGIC   type string
# MAGIC )
# MAGIC using delta
# MAGIC location 'file:/Workspace/Users/nikita.ivanov@quantori.com/pdbs/silver/entity';
# MAGIC
# MAGIC insert into silver_entity by name
# MAGIC select 
# MAGIC   experiment,
# MAGIC   id,
# MAGIC   details,
# MAGIC   formula_weight::float,
# MAGIC   pdbx_description,
# MAGIC   pdbx_ec ,
# MAGIC   pdbx_entities_per_biological_unit::float,
# MAGIC   pdbx_formula_weight_exptl::float,
# MAGIC   pdbx_formula_weight_exptl_method,
# MAGIC   pdbx_fragment,
# MAGIC   pdbx_modification,
# MAGIC   pdbx_mutation,
# MAGIC   pdbx_number_of_molecules::int,
# MAGIC   pdbx_parent_entity_id ,
# MAGIC   pdbx_target_id ,
# MAGIC   src_method ,
# MAGIC   type 
# MAGIC from bronze_entity
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_entity;

# COMMAND ----------

from pyspark.sql.functions import col
import pyspark
    
df = (spark
    .table("raw_pdb")
    .select("*")
    )
#display(df)

for col in df.columns:
    single_col = df.select(col)
    #display(single_col)

display(df.select('_entity'))

mydict = {} 

df = df.select('_entity').toPandas() 

for column in df.columns: 
    mydict[column] = df[column].values.tolist() 

print(mydict)

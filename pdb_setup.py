# Databricks notebook source
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

experiment_name = '100d'


resp = r.get('https://files.wwpdb.org/pub/pdb/data/structures/all/mmCIF/' + experiment_name + '.cif.gz')
experiment_name.upper()
print(resp.status_code)
file_content_str = g.decompress(resp.content)
etag = resp.headers['ETag']


import os
import json

cwd = os.getcwd()
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
# MAGIC ###Lets try to get dict data from website...

# COMMAND ----------

import bs4 
import pprint 
from pprint import pprint
import requests as r
import json

categories = ['entity', 'pdbx_database_PDB_obs_spr', 'entity_poly_seq', 'chem_comp']
attributes = ['exptl.entry_id', 'exptl.method']

categories_site = 'https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Categories/'
link_prefix = 'https://mmcif.wwpdb.org'


categories_dict = {}
for category in categories:
    category_attributes = []
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
                category_attributes.append((link_prefix + href_itself,href_text))
    categories_dict[category] = category_attributes

pprint(categories_dict)

with open('/Workspace/Users/nikita.ivanov@quantori.com/pdb_pipeline_1/PDB_dict/schema.json', 'w') as fp:
    json.dump(categories_dict, fp)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Futile attempt to parse PDB dictionary to get data schema 

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
# MAGIC ###Old code, we want to enrich and validate data from dictionary

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
        a = cif_dict[experiment_name][key]
        if key == '_entity':
            a = full_entity | a
            print(a)
        our_stuff.update({key:a})

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
# MAGIC create table if not exists bronze_entity
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
# MAGIC using delta
# MAGIC location 'file:/Workspace/Users/nikita.ivanov@quantori.com/pdbs/bronze/entity';
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

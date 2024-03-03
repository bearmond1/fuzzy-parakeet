# About
This is a data engineering project, which retieves and processes data from [Worldwide Protein Data Bank](https://www.wwpdb.org/). We dealing with data about 3D structures of proteins, nucleic acids, etc. Data present at PDB server in CIF format. Goal is to bring it to structured state of tables for each category. In this project we focusing on categories: entity, pdbx_database_PDB_obs_sp, entity_poly_seq, chem_comp, exptl.

# Source data
Largest unit of data is experiment, represented by code of 4 symbols, like '100D'. Data presented in mmCIF (Macromolecular Crystallographic Information File) format. Each experiment contains number of categories, each category is either set of key-value pairs or a table of values. In mmCIF format list of keys precedes actual values, like so:

Key : value category:
![Key : value category](/screenshots/key_value_cat.png) 

Table category:
![Table category](/screenshots/table_cat.png)

For parsing CIF data we`re using [PDBeCIF parser](https://pdbeurope.github.io/pdbecif/index.html#). It provides us with easy convertion CIF -> json. 

# Workflow
1) We start off deciding which experiments needs update or initial load. File server provides ETag - id of current version, before downloading data we can check if we already have latest version.
2) Required CIF files are being written in storage
3) We parse CIF format to json, and write it adding etag version - we can run queries on json to check etag varsion.
4) Each category goes into its own table, so we extract category from json file and work with it only.
5) We got a json with list of future columns as keys, and in values we got arrays of data for future rows. Spark dataframe does not understand this format, unlike pandas. So we create pandas dataframe from json, and then spark dataframe from pandas. 
6) Our task makes target table scheme defined - table for each category, column per attribute, plus column for experiment name. However, some categories and attributes can be not present in data. For each categry missing we`re going to add column in spark dataframe.
7) Now we are ready to write dataframe into table of bronze layer. After that we have to replace all '?' with Nulls in our table, since mmCIF format will have them on place of missing value.
8) We arrived at place where data came to structured view, we shouldn't have lost any data by this point. Now it's time to clean data. PDBx/mmCIF dictionary provides us with information about attributes like it's data type, list of allowed values, if it's obligatory. We fill our silver table with data, which passed all these checks.

Voilà !

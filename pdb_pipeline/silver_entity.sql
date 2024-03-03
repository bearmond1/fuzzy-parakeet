-- Databricks notebook source
insert into pdb_pipeline.silver_entity
select 
  experiment,
  id, -- char, obligatory
  details,
  formula_weight::float , -- float
  pdbx_description ,
  pdbx_ec , 
  pdbx_entities_per_biological_unit::float , -- float
  pdbx_formula_weight_exptl::float , -- float
  pdbx_formula_weight_exptl_method ,
  pdbx_fragment , 
  pdbx_modification ,
  pdbx_mutation ,
  pdbx_number_of_molecules::int, -- int
  pdbx_parent_entity_id ,
  pdbx_target_id ,
  src_method , -- [man, nat, syn]
  type -- [branched,	macrolide,	non-polymer,	polymer,	water]
from pdb_pipeline.bronze_entity  
where 
  experiment is not null and
  id is not null and
  ( formula_weight is null or formula_weight::float is not null ) and
  ( pdbx_entities_per_biological_unit is null or pdbx_entities_per_biological_unit::float is not null ) and
  ( pdbx_formula_weight_exptl is null or pdbx_formula_weight_exptl::float is not null ) and
  ( pdbx_number_of_molecules is null or pdbx_number_of_molecules::int is not null ) and
  ( src_method is null or src_method in ('man', 'nat', 'syn') ) and
  ( type is null or type in ('branched',	'macrolide',	'non-polymer',	'polymer',	'water') )


-- COMMAND ----------

select * from pdb_pipeline.silver_entity;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC create or replace table pdb_pipeline.silver_entity  (
-- MAGIC   experiment string not null,
-- MAGIC   id  string not null, 
-- MAGIC   details string,
-- MAGIC   formula_weight float , 
-- MAGIC   pdbx_description string,
-- MAGIC   pdbx_ec string, 
-- MAGIC   pdbx_entities_per_biological_unit float , 
-- MAGIC   pdbx_formula_weight_exptl float ,
-- MAGIC   pdbx_formula_weight_exptl_method string,
-- MAGIC   pdbx_fragment string, 
-- MAGIC   pdbx_modification string,
-- MAGIC   pdbx_mutation string,
-- MAGIC   pdbx_number_of_molecules int,
-- MAGIC   pdbx_parent_entity_id string,
-- MAGIC   pdbx_target_id string,
-- MAGIC   src_method string,
-- MAGIC   type string
-- MAGIC )
-- MAGIC using delta;

-- Databricks notebook source
merge into pdb_pipeline.silver_pdbx_database_PDB_obs_spr
using (
select
  experiment ,
  pdb_id ,
  replace_pdb_id ,
  date::date,
  details  ,
  id
from pdb_pipeline.bronze_pdbx_database_PDB_obs_spr
where 
  experiment in (select experiment from pdb_pipeline.current_run_categories where category = 'pdbx_db_PDB_obs_spr' ) and
  experiment is not null and
  pdb_id is not null and
  replace_pdb_id is not null and
  date is not null and
  date::date is not null and
  id is not null
) as source
on 
pdb_pipeline.silver_pdbx_database_PDB_obs_spr.experiment = source.experiment and
pdb_pipeline.silver_pdbx_database_PDB_obs_spr.pdb_id = source.pdb_id and
pdb_pipeline.silver_pdbx_database_PDB_obs_spr.replace_pdb_id = source.replace_pdb_id

when matched then update set *
when not matched then insert *
;

-- COMMAND ----------

merge into pdb_pipeline.silver_pdbx_database_PDB_obs_spr
using (
select
  experiment ,
  pdb_id ,
  replace_pdb_id ,
  date::date,
  details  ,
  id
from pdb_pipeline.bronze_pdbx_database_PDB_obs_spr
where 
  experiment in (select experiment from pdb_pipeline.current_run_categories where category = 'pdbx_db_PDB_obs_spr' ) and
  not (
    experiment is not null and
    pdb_id is not null and
    replace_pdb_id is not null and
    date is not null and
    date::date is not null and
    id is not null
  )
) as source
on 
pdb_pipeline.silver_pdbx_database_PDB_obs_spr.experiment = source.experiment and
pdb_pipeline.silver_pdbx_database_PDB_obs_spr.pdb_id = source.pdb_id and
pdb_pipeline.silver_pdbx_database_PDB_obs_spr.replace_pdb_id = source.replace_pdb_id

when matched then update set *
when not matched then insert *
;

-- COMMAND ----------

select * from pdb_pipeline.silver_pdbx_database_PDB_obs_spr
where 
  experiment in (select experiment from pdb_pipeline.current_run_categories where category = 'pdbx_db_PDB_obs_spr' ) limit 100;

-- COMMAND ----------

select * from pdb_pipeline.rejected_pdbx_database_PDB_obs_spr
where 
  experiment in (select experiment from pdb_pipeline.current_run_categories where category = 'pdbx_db_PDB_obs_spr' ) limit 100;

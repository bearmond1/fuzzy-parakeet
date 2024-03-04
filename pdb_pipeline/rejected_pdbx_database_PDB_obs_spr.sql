-- Databricks notebook source
insert into pdb_pipeline.rejected_pdbx_database_PDB_obs_spr
select
  experiment ,
  pdb_id ,
  replace_pdb_id ,
  date::date,
  details  ,
  id
from pdb_pipeline.bronze_pdbx_database_PDB_obs_spr
where 
  experiment in (select expriment from pdb_pipeline.current_run_categories where category = 'pdbx_db_PDB_obs_spr') and
not (
  experiment is not null and
  pdb_id is not null and
  replace_pdb_id is not null and
  date is not null and
  date::date is not null and
  id is not null )
  ;

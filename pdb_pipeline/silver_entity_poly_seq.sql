-- Databricks notebook source
merge into pdb_pipeline.silver_entity_poly_seq
using (
select distinct
  experiment,
  entity_id ,
  mon_id ,
  num ,
  hetero 
from pdb_pipeline.bronze_entity_poly_seq 
where
  experiment in (select experiment from pdb_pipeline.current_run_categories where category = 'entity_poly_seq') and
  experiment is not null and
  entity_id is not null and
  mon_id is not null and
  num::int is not null and
  ( hetero in ('n','no','y','yes') or hetero is null )
) as source
on 
  pdb_pipeline.silver_entity_poly_seq.experiment = source.experiment and
  pdb_pipeline.silver_entity_poly_seq.entity_id = source.entity_id and
  pdb_pipeline.silver_entity_poly_seq.mon_id = source.mon_id and
  pdb_pipeline.silver_entity_poly_seq.num = source.num

when matched then update set *
when not matched then insert *


-- COMMAND ----------

merge into pdb_pipeline.rejected_entity_poly_seq
using (
select distinct
  experiment,
  entity_id ,
  mon_id ,
  num ,
  hetero 
from pdb_pipeline.bronze_entity_poly_seq 
where
  experiment in (select experiment from pdb_pipeline.current_run_categories where category = 'entity_poly_seq') and
  not (
  experiment is not null and
  entity_id is not null and
  mon_id is not null and
  num::int is not null and
  ( hetero in ('n','no','y','yes') or hetero is null ) )
) as source
on 
  pdb_pipeline.rejected_entity_poly_seq.experiment = source.experiment and
  pdb_pipeline.rejected_entity_poly_seq.entity_id = source.entity_id and
  pdb_pipeline.rejected_entity_poly_seq.mon_id = source.mon_id and
  pdb_pipeline.rejected_entity_poly_seq.num = source.num

when matched then update set *
when not matched then insert *


-- COMMAND ----------

select * 
from pdb_pipeline.silver_entity_poly_seq
where
  experiment in (select experiment from pdb_pipeline.current_run_categories where category = 'entity_poly_seq')
limit 100;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC truncate table pdb_pipeline.silver_entity_poly_seq;
-- MAGIC truncate table pdb_pipeline.rejected_entity_poly_seq;

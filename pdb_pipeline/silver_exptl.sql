-- Databricks notebook source
merge into pdb_pipeline.silver_exptl
using (
select distinct
  experiment,
  entry_id,
  method
from pdb_pipeline.bronze_exptl
where 
  experiment in (select experiment from pdb_pipeline.current_run_categories ) and
  entry_id is not null and
  method in (
    'ELECTRON CRYSTALLOGRAPHY',
    'ELECTRON MICROSCOPY',
    'EPR	EPR',
    'FIBER DIFFRACTION',
    'FLUORESCENCE TRANSFER	FLUORESCENCE TRANSFER',
    'INFRARED SPECTROSCOPY	IR and FTIR',
    'NEUTRON DIFFRACTION',
    'POWDER DIFFRACTION',
    'SOLID-STATE NMR',
    'SOLUTION NMR',
    'SOLUTION SCATTERING',
    'THEORETICAL MODEL	THEORETICAL MODEL',
    'X-RAY DIFFRACTION')
) as source
  on pdb_pipeline.silver_exptl.experiment = source.experiment and
     pdb_pipeline.silver_exptl.entry_id = source.entry_id and
     pdb_pipeline.silver_exptl.method = source.method
when matched then update set *
when not matched then insert *

-- COMMAND ----------

merge into pdb_pipeline.rejected_exptl
using (
select distinct
  experiment,
  entry_id,
  method
from pdb_pipeline.bronze_exptl
where 
  experiment in (select experiment from pdb_pipeline.current_run_categories ) and
  not (
  entry_id is not null and
  method in (
    'ELECTRON CRYSTALLOGRAPHY',
    'ELECTRON MICROSCOPY',
    'EPR	EPR',
    'FIBER DIFFRACTION',
    'FLUORESCENCE TRANSFER	FLUORESCENCE TRANSFER',
    'INFRARED SPECTROSCOPY	IR and FTIR',
    'NEUTRON DIFFRACTION',
    'POWDER DIFFRACTION',
    'SOLID-STATE NMR',
    'SOLUTION NMR',
    'SOLUTION SCATTERING',
    'THEORETICAL MODEL	THEORETICAL MODEL',
    'X-RAY DIFFRACTION')
  )
) as source
  on pdb_pipeline.rejected_exptl.experiment = source.experiment and
     pdb_pipeline.rejected_exptl.entry_id = source.entry_id and
     pdb_pipeline.rejected_exptl.method = source.method
when matched then update set *
when not matched then insert *


-- COMMAND ----------

select * from pdb_pipeline.silver_exptl;
--truncate table pdb_pipeline.rejected_exptl;
--truncate table pdb_pipeline.silver_exptl;

-- Databricks notebook source
merge into pdb_pipeline.silver_chem_comp
using (
select distinct
  experiment ,
  id ,
  formula ,
  formula_weight::float,
  model_details ,
  model_erf ,
  model_source ,
  mon_nstd_class ,
  mon_nstd_details ,
  mon_nstd_flag , -- n,no,y,yes
  mon_nstd_parent ,
  mon_nstd_parent_comp_id ,
  name ,
  number_atoms_all::int,
  number_atoms_nh::int,
  one_letter_code ,
  pdbx_ambiguous_flag ,
  pdbx_casnum ,
  pdbx_class_1 ,
  pdbx_class_2 ,
  pdbx_comp_type , -- inorganic ligand, metal cation, organic ligand, organometalic ligand, solvent
  pdbx_component_no ,
  pdbx_formal_charge::int,
  pdbx_ideal_coordinates_details ,
  pdbx_ideal_coordinates_missing_flag , -- Y,N
  pdbx_initial_date::date,
  pdbx_model_coordinates_db_code ,
  pdbx_model_coordinates_details ,
  pdbx_model_coordinates_missing_flag , -- Y,N
  pdbx_modification_details ,
  pdbx_modified_date::date,
  pdbx_nscnum ,
  pdbx_number_subcomponents::int,
  pdbx_pcm , -- Y,N
  pdbx_processing_site , -- EBI,PDBC,PDBE,PDBJ,RCSB
  pdbx_release_status , -- DEL,HOLD,HPUB,OBS,REF_ONLY,REL
  pdbx_replaced_by ,
  pdbx_replaces ,
  pdbx_reserved_name ,
  pdbx_smiles ,
  pdbx_status ,
  pdbx_subcomponent_list ,
  pdbx_synonyms ,
  pdbx_type ,
  pdbx_type_modified , -- 0,1
  three_letter_code ,
  type 
from pdb_pipeline.bronze_chem_comp
where 
  experiment in (select experiment from pdb_pipeline.current_run_categories where category = 'chem_comp' ) and
  experiment is not null and
  id is not null and
  type is not null and

  ( formula_weight is null or formula_weight::float is not null ) and  
  ( number_atoms_all is null or number_atoms_all::int is not null ) and  
  ( number_atoms_nh is null or number_atoms_nh::int is not null ) and  
  ( pdbx_formal_charge is null or pdbx_formal_charge::int is not null ) and  
  ( pdbx_initial_date is null or pdbx_initial_date::date is not null ) and  
  ( pdbx_modified_date is null or pdbx_modified_date::date is not null ) and  
  ( pdbx_number_subcomponents is null or pdbx_number_subcomponents::int is not null ) and  

  ( mon_nstd_flag in ('n','no','y','yes') or mon_nstd_flag is null ) and
  ( pdbx_comp_type in ('inorganic ligand', 'metal cation', 'organic ligand', 'organometalic ligand', 'solvent') or pdbx_comp_type is null ) and
  ( pdbx_ideal_coordinates_missing_flag in ('Y','N') or pdbx_ideal_coordinates_missing_flag is null ) and
  ( pdbx_model_coordinates_missing_flag in ('Y','N') or pdbx_model_coordinates_missing_flag is null ) and
  ( pdbx_pcm in ('Y','N') or pdbx_pcm is null ) and
  ( pdbx_processing_site in ('EBI','PDBC','PDBE','PDBJ','RCSB') or pdbx_processing_site is null ) and
  ( pdbx_release_status is null or pdbx_release_status in ('DEL','HOLD','HPUB','OBS','REF_ONLY','REL') ) and
  ( pdbx_type_modified is null or pdbx_type_modified in ('0', '1')) and
  type in ('D-beta-peptide, C-gamma linking', 'D-gamma-peptide, C-delta linking', 'D-peptide COOH carboxy terminus', 'D-peptide NH3 amino terminus', 'D-peptide linking', 'D-saccharide', 'D-saccharide, alpha linking', 'D-saccharide, beta linking', 'DNA OH 3 prime terminus', 'DNA OH 5 prime terminus', 'DNA linking', 'L-DNA linking', 'L-RNA linking', 'L-beta-peptide, C-gamma linking', 'L-gamma-peptide, C-delta linking', 'L-peptide COOH carboxy terminus', 'L-peptide NH3 amino terminus', 'L-peptide linking', 'L-saccharide', 'L-saccharide, alpha linking', 'L-saccharide, beta linking', 'RNA OH 3 prime terminus', 'RNA OH 5 prime terminus', 'RNA linking', 'non-polymer', 'other', 'peptide linking', 'peptide-like', 'saccharide')
) as source
on pdb_pipeline.silver_chem_comp.experiment = source.experiment and
   pdb_pipeline.silver_chem_comp.id = source.id
when matched then update set *
when not matched then insert *

-- COMMAND ----------

merge into pdb_pipeline.rejected_chem_comp
using (
select distinct
  experiment ,
  id ,
  formula ,
  formula_weight::float,
  model_details ,
  model_erf ,
  model_source ,
  mon_nstd_class ,
  mon_nstd_details ,
  mon_nstd_flag , -- n,no,y,yes
  mon_nstd_parent ,
  mon_nstd_parent_comp_id ,
  name ,
  number_atoms_all::int,
  number_atoms_nh::int,
  one_letter_code ,
  pdbx_ambiguous_flag ,
  pdbx_casnum ,
  pdbx_class_1 ,
  pdbx_class_2 ,
  pdbx_comp_type , -- inorganic ligand, metal cation, organic ligand, organometalic ligand, solvent
  pdbx_component_no ,
  pdbx_formal_charge::int,
  pdbx_ideal_coordinates_details ,
  pdbx_ideal_coordinates_missing_flag , -- Y,N
  pdbx_initial_date::date,
  pdbx_model_coordinates_db_code ,
  pdbx_model_coordinates_details ,
  pdbx_model_coordinates_missing_flag , -- Y,N
  pdbx_modification_details ,
  pdbx_modified_date::date,
  pdbx_nscnum ,
  pdbx_number_subcomponents::int,
  pdbx_pcm , -- Y,N
  pdbx_processing_site , -- EBI,PDBC,PDBE,PDBJ,RCSB
  pdbx_release_status , -- DEL,HOLD,HPUB,OBS,REF_ONLY,REL
  pdbx_replaced_by ,
  pdbx_replaces ,
  pdbx_reserved_name ,
  pdbx_smiles ,
  pdbx_status ,
  pdbx_subcomponent_list ,
  pdbx_synonyms ,
  pdbx_type ,
  pdbx_type_modified , -- 0,1
  three_letter_code ,
  type 
from pdb_pipeline.bronze_chem_comp
where 
  experiment in (select experiment from pdb_pipeline.current_run_categories where category = 'chem_comp' ) and
  not (
    experiment is not null and
    id is not null and
    type is not null and

    ( formula_weight is null or formula_weight::float is not null ) and  
    ( number_atoms_all is null or number_atoms_all::int is not null ) and  
    ( number_atoms_nh is null or number_atoms_nh::int is not null ) and  
    ( pdbx_formal_charge is null or pdbx_formal_charge::int is not null ) and  
    ( pdbx_initial_date is null or pdbx_initial_date::date is not null ) and  
    ( pdbx_modified_date is null or pdbx_modified_date::date is not null ) and  
    ( pdbx_number_subcomponents is null or pdbx_number_subcomponents::int is not null ) and  

    ( mon_nstd_flag in ('n','no','y','yes') or mon_nstd_flag is null ) and
    ( pdbx_comp_type in ('inorganic ligand', 'metal cation', 'organic ligand', 'organometalic ligand', 'solvent') or pdbx_comp_type is null ) and
    ( pdbx_ideal_coordinates_missing_flag in ('Y','N') or pdbx_ideal_coordinates_missing_flag is null ) and
    ( pdbx_model_coordinates_missing_flag in ('Y','N') or pdbx_model_coordinates_missing_flag is null ) and
    ( pdbx_pcm in ('Y','N') or pdbx_pcm is null ) and
    ( pdbx_processing_site in ('EBI','PDBC','PDBE','PDBJ','RCSB') or pdbx_processing_site is null ) and
    ( pdbx_release_status is null or pdbx_release_status in ('DEL','HOLD','HPUB','OBS','REF_ONLY','REL') ) and
    ( pdbx_type_modified is null or pdbx_type_modified in ('0', '1')) and
    type in ('D-beta-peptide, C-gamma linking', 'D-gamma-peptide, C-delta linking', 'D-peptide COOH carboxy terminus', 'D-peptide NH3 amino terminus', 'D-peptide linking', 'D-saccharide', 'D-saccharide, alpha linking', 'D-saccharide, beta linking', 'DNA OH 3 prime terminus', 'DNA OH 5 prime terminus', 'DNA linking', 'L-DNA linking', 'L-RNA linking', 'L-beta-peptide, C-gamma linking', 'L-gamma-peptide, C-delta linking', 'L-peptide COOH carboxy terminus', 'L-peptide NH3 amino terminus', 'L-peptide linking', 'L-saccharide', 'L-saccharide, alpha linking', 'L-saccharide, beta linking', 'RNA OH 3 prime terminus', 'RNA OH 5 prime terminus', 'RNA linking', 'non-polymer', 'other', 'peptide linking', 'peptide-like', 'saccharide')
  )
) as source
on pdb_pipeline.rejected_chem_comp.experiment = source.experiment and
   pdb_pipeline.rejected_chem_comp.id = source.id
when matched then update set *
when not matched then insert *

-- COMMAND ----------

select * from pdb_pipeline.rejected_chem_comp
where
  experiment in (select experiment from pdb_pipeline.current_run_categories where category = 'chem_comp' ) ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC select experiment from pdb_pipeline.current_run_categories where category = 'chem_comp';

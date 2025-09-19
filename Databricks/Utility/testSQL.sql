-- Databricks notebook source
select * from qlt.masterdata_silver_master_plant.0PLANT_TEXT version as of 663 silver
left outer join qlt.masterdata_silver_master_plant.0PLANT_TEXT version as of 664 silver_delete on silver.WERKS = silver_delete.WERKS
where silver_delete.WERKS is null

-- COMMAND ----------

DESCRIBE HISTORY prd.masterdata_silver_master_plant.0PLANT_TEXT

-- COMMAND ----------

DESCRIBE HISTORY qlt.masterdata_silver_master_plant.0PLANT_TEXT

-- COMMAND ----------


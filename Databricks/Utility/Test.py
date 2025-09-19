# Databricks notebook source
import os

from math import (
  floor
)

from typing import (
  Dict,
  Union,
  List
)

import pyspark

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pyspark.errors import (AnalysisException)

from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Shared_Projects/S4_Remediation/Utility/StructTypeDefinition

# COMMAND ----------

# MAGIC %run /Workspace/Shared_Projects/S4_Remediation/Utility/PromotionFunctionDefinition

# COMMAND ----------

functional_domain = 'finance'
functional_sub_domain = 'accounting'
data_type = 'fact'
data_source = 'sapeccfe3'
entity = 'general_ledger'
ingestion_type = 'delta'
sub_entity = '0FI_GL_4'

# COMMAND ----------

struct_type_current = struct_type_definition[functional_domain][functional_sub_domain][entity][sub_entity]
promotion_function_current = promotion_function_definition[functional_domain][functional_sub_domain][entity][sub_entity]
primary_keys, raw_schema, bronze_schema, silver_schema = struct_type_current.values()
bronze_func, silver_func = promotion_function_current.values()

# COMMAND ----------

sequence_number_column = 'DI_SEQUENCE_NUMBER'

# COMMAND ----------

df = spark.read.options(delimiter="|", header=True).csv("/Volumes/qlt_finance/accounting_raw_fact_sapeccfe3_general_ledger/delta/0FI_GL_4/historical_initial_load/error/0FI_GL_4_2025.09.02_114139.csv")

# COMMAND ----------

df_primary_keys = df.select(primary_keys.names).groupby(primary_keys.names).agg(F.count("*").alias("count_primary_keys")).alias("df_primary_keys")

# COMMAND ----------

df_duplicated = df_primary_keys.where(F.col("count_primary_keys") > 1).alias("df_duplicated")

# COMMAND ----------

df.join(df_duplicated, primary_keys.names, "inner").display()

# COMMAND ----------

df = spark.read.table("prd.masterdata_bronze_master_product_documentation.sapeccfe3_zimm_rdp_output").where(F.col("RDPNR") == '631221').display()

# COMMAND ----------

df = spark.read.table("qlt.masterdata_bronze_master_product_documentation.sapeccfe3_zimm_rdp_output").where(F.col("RDPNR") == '631221').display()

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history prd.masterdata_bronze_master_product_documentation.sapeccfe3_zimm_rdp_output

# COMMAND ----------

df = spark.read.option("delimiter", ";").option("header", True).csv("abfss://masterdata@sabiferreroprd.dfs.core.windows.net/bronze/master/sapeccfe3/product_documentation/input/ZIMM_RDP/202509/ZIMM_RDP_20250918_1048.csv")

# COMMAND ----------

df_qlt = spark.read.table("qlt_finance.accounting_bronze_fact_sapeccfe3_general_ledger.0FI_GL_4")
f"{df_qlt.count():,}"

# COMMAND ----------

df_prd = spark.read.table("prd_finance.accounting_silver_fact_general_ledger.0FI_GL_4")
f"{df_prd.count():,}"

# COMMAND ----------

df_control = df_qlt.alias("qlt").join(
  df_prd.alias("prd"),
  primary_keys.names,
  "inner"
).select(primary_keys.names + [f.col("qlt.HKONT").alias("qlt_HKONT"), f.col("prd.HKONT").alias("prd_HKONT")])

df_check = df_control.where(f.col("qlt_HKONT") != f.col("prd_HKONT"))
f"Righe diverse per HKONT: {df_check.count():,}"


# COMMAND ----------


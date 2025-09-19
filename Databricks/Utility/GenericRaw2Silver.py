# Databricks notebook source
# dbutils.widgets.text("FunctionalDomain", "","Functional Domain")
# dbutils.widgets.text("FunctionalSubDomain", "","Functional Sub Domain")
# dbutils.widgets.text("DataType", "","Data Type")
# dbutils.widgets.text("DataSource", "","Data Source")
# dbutils.widgets.text("Entity", "","Entity")
# dbutils.widgets.text("IngestionType", "","Ingestion Type")
# dbutils.widgets.text("SubEntity", "","Sub Entity")

# COMMAND ----------

functional_domain = dbutils.widgets.get("FunctionalDomain")
functional_sub_domain = dbutils.widgets.get("FunctionalSubDomain")
data_type = dbutils.widgets.get("DataType")
data_source = dbutils.widgets.get("DataSource")
entity = dbutils.widgets.get("Entity")
ingestion_type = dbutils.widgets.get("IngestionType")
sub_entity = dbutils.widgets.get("SubEntity")

# COMMAND ----------

# MAGIC %run /Workspace/Shared_Projects/S4_Remediation/Utility/StructTypeDefinition

# COMMAND ----------

# MAGIC %run /Workspace/Shared_Projects/S4_Remediation/Utility/PromotionFunctionDefinition

# COMMAND ----------

# MAGIC %run /Workspace/Shared_Projects/S4_Remediation/Utility/Entity_Class

# COMMAND ----------

struct_type_current = struct_type_definition[functional_domain][functional_sub_domain][entity][sub_entity]
promotion_function_current = promotion_function_definition[functional_domain][functional_sub_domain][entity][sub_entity]
primary_keys, raw_schema, bronze_schema, silver_schema = struct_type_current.values()
bronze_func, silver_func = promotion_function_current.values()

# COMMAND ----------

entity_obj = Entity(
  functional_domain = functional_domain,
  functional_sub_domain = functional_sub_domain,
  data_type = data_type,
  data_source = data_source,
  entity = entity,
  ingestion_type = ingestion_type,
  sub_entity = sub_entity,
  env='qlt'
)

# COMMAND ----------

df = entity_obj.get_raw_files("historical_initial_load/processed")
print(f"{get_local_time()} - SubEntity[{entity_obj.sub_entity}] - raw files: {df.count()}")
# for path, fname in [(row["path"], row["name"]) for row in df.collect()]:
#   dbutils.fs.mv(path, "dbfs:/Volumes/qlt_finance/accounting_raw_fact_sapeccfe3_general_ledger/delta/0FI_GL_4/historical_initial_load/" + fname)

# COMMAND ----------

# spark.sql(f"DELETE FROM {entity_obj.bronze_schema_name}")
# spark.sql(f"DELETE FROM {entity_obj.silver_schema_name}")

# print(spark.read.table(entity_obj.bronze_schema_name).count())
# print(spark.read.table(entity_obj.silver_schema_name).count())

# COMMAND ----------

entity_obj.raw2Bronze(
  raw_schema = raw_schema,
  primary_keys = primary_keys,
  additional_path={
    "additional_raw_volume_path": "historical_initial_load/",
    "additional_archive_path": "historical_initial_load/processed/",
    "additional_error_path": "historical_initial_load/error/"
  },
  sequence_number_column = 'DI_SEQUENCE_NUMBER',
  bronze_schema = bronze_schema
)

# COMMAND ----------

# MAGIC %run "/Workspace/Shared_Projects/S4_Remediation/Utility/Utility Function"

# COMMAND ----------


from pyspark.sql.types import FloatType

def convert_time_in_label(second : float) -> str:
  """
  Parameters
  ----------
  bytes : float
    Value to convert.
  
  Returns
  str
    A label in more readble format of a big number
  """
  return convert_float_to_label(second * 1000, {
      "millisecond": {"standard_value": 1000},
      "second": {"standard_value": 60}, 
      "minute": {"standard_value": 60},
      "hour": {"standard_value": 24},
      "day": {"standard_value": float('inf')}
    }
  )

to_second_udf = F.udf(lambda time_delta: time_delta.total_seconds(), FloatType()).asNondeterministic()
to_time_str_udf = F.udf(lambda s: convert_time_in_label(s), StringType()).asNondeterministic()

# COMMAND ----------

entity_obj.df_time.withColumn("duration(second)", to_second_udf(F.col("end_time") - F.col("start_time"))).groupby(F.col("activity")).agg(
  F.sum(F.col("duration(second)")).alias("total duration(second)"),
  F.avg(F.col("duration(second)")).alias("avg duration(second)"),
  F.count("*").alias("total files")
).withColumn("total duration time", to_time_str_udf(F.col("total duration(second)"))
).withColumn( "avg duration time", to_time_str_udf(F.col("avg duration(second)"))
).display()

# COMMAND ----------

entity_obj.df_time.withColumn("duration(second)", to_second_udf(F.col("end_time") - F.col("start_time"))).agg(
  F.sum(F.col("duration(second)")).alias("total duration(second)"),
  F.avg(F.col("duration(second)")).alias("avg duration(second)"),
  F.count("*").alias("total files")
).withColumn("total duration time", to_time_str_udf(F.col("total duration(second)"))
).withColumn( "avg duration time", to_time_str_udf(F.col("avg duration(second)"))
).display()

# COMMAND ----------

entity_obj.bronze2Silver(
  write_mode = 'merge',
  operation_type_column = 'DI_OPERATION_TYPE',
  sequence_number_column = 'DI_SEQUENCE_NUMBER',
  primary_keys = primary_keys,
  silver_schema = silver_schema,
  silver_func = silver_func,
  f_args=[data_source]
)
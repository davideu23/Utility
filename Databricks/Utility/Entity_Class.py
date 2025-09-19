# Databricks notebook source
# MAGIC %md
# MAGIC #Import

# COMMAND ----------

import os, datetime, time

from math import (
  floor
)

from typing import (
  Dict,
  Union,
  List
)

import pyspark, pytz

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pyspark.sql.types import (
  StructType,
  StructField,
  StringType,
  DoubleType,
  TimestampType,
  LongType
)

from pyspark.errors import (AnalysisException)

from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC #Error Classes

# COMMAND ----------

"""
severity = 00 -> Critical Error -> Stop Execution of entire Flow
severity = 10 -> Error -> Stop Execution of current step
severity = 20 -> Warning -> Continue Execution but log the error
severity = 30 -> Info -> Log the information
"""

class DataFrameEmptyError(Exception):
  def __init__(self, message, *args, **kwargs):
    super().__init__(message, *args, **kwargs)
    self.severity = "10"
    self.errorCode = "EmptyError"
class DataFrameKeysNullError(Exception):
  def __init__(self, message, *args, **kwargs):
    super().__init__(message, *args, **kwargs)
    self.severity = "10"
    self.errorCode = "KeysNull"
class DataFrameDuplicateFoundError(Exception): 
  def __init__(self, message, *args, **kwargs):
    super().__init__(message, *args, **kwargs)
    self.severity = "10"
    self.errorCode = "DuplicatedFound"
class DataFrameDuplicateRecordNumberWarning(Exception): 
  def __init__(self, message, *args, **kwargs):
    super().__init__(message, *args, **kwargs)
    self.severity = "20"
    self.errorCode = "DuplicatedFound"

# COMMAND ----------

# MAGIC %md
# MAGIC #Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ##get_local_time

# COMMAND ----------

def get_local_time(tz : str = "Europe/Rome", return_type : str = "str"):
  """
  
  Parameters
  ----------
  tz : str
    Timezone to use
  return_type : str, default "str"
    Return type of the function. Can be "str" or "datetime".
  """
  local_time = datetime.datetime.now(tz=pytz.timezone(tz))
  if return_type == "datetime": return local_time
  if return_type == "str": return local_time.strftime("%Y-%m-%d %H:%M:%S.%f %Z%z")
  raise ValueError(f"Return type {return_type} not supported.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##data_quality_check

# COMMAND ----------

def data_quality_check(
  df_raw : pyspark.sql.dataframe.DataFrame, 
  primary_keys : pyspark.sql.types.StructType, 
  sequence_number_column : str,
  file_name : str
  ) -> None:
  """
  Perform data quality check for the specified dataframe and raise error if any of the check fails

  Parameters
  ----------
  df_raw : pyspark.sql.dataframe.DataFrame
    Dataframe to check
  primary_keys : pyspark.sql.types.StructType
    Primary keys of the table
  sequence_number_column : str
    Column to use to decide with which record to keep if there are duplicates
  file_name : str
    Name of the file to be processed

  Returns
  -------
  None
  """
  if df_raw.count() == 0:
    raise DataFrameEmptyError(f"File contains no record")

  df_not_null = df_raw.dropna(how="all", subset=primary_keys.names).alias("df_not_null")
  if df_not_null.count() < df_raw.count():
    raise DataFrameKeysNullError(f"File contains record with null values for all primary keys")

  df_duplicated = df_raw.select(primary_keys.names).dropna(
    how="any", 
    subset=primary_keys.names
  ).groupby(primary_keys.names).agg(F.count("*").alias("cnt_primary_keys")).alias("df_duplicated_primary_keys")
  df_duplicated = df_duplicated.where(F.col("cnt_primary_keys") > 1)
  if df_duplicated.count() > 0:
    df_duplicated_record_number = df_raw.select(primary_keys.names + [sequence_number_column]).dropna(
      how="any", 
      subset=primary_keys.names + [sequence_number_column]
    ).groupby(primary_keys.names + [sequence_number_column]).agg(F.count("*").alias("cnt_record_number")).alias("df_duplicated_record_number")
    df_duplicated_record_number = df_duplicated_record_number.where(F.col("cnt_record_number") > 1)
    if df_duplicated_record_number.count() > 0:
      raise DataFrameDuplicateFoundError(f"File contains duplicate records on keys")
    raise DataFrameDuplicateRecordNumberWarning(f"File contains duplicate records for same key and {sequence_number_column}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##convert_float_to_label

# COMMAND ----------

def convert_float_to_label(start_value: float, divisor : Dict[str, any]) -> str:
  """
  Parameters
  ----------
  start_value : float
    Value to convert.
  divisor : Dict[str, any]
    What are the divisors use to obtain the next value unit
    example:
    bytes_conversion = {
      "byte": {"standard_value": 1024}, 
      "kilobyte": {"standard_value": 1024},
      "megabyte": {"standard_value": 1024},
      "gigabyte": {"standard_value": 1024}
    }
    time_conversion = {
      "second": {"standard_value": 60}, 
      "minute": {"standard_value": 60},
      "hour": {"standard_value": 24},
      "day": {"standard_value": float('inf')}
    }

  Returns
  -------
  str
    A label in more readble format of a big number
  """
  
  for value_part, properties in divisor.items():
    if start_value >= properties["standard_value"]:
      current_value = start_value / properties["standard_value"]
      properties["value"] = int((current_value - floor(current_value)) * properties["standard_value"])
      start_value = (start_value - properties["value"]) / properties["standard_value"]
    else: 
      properties["value"] = int(start_value)
      break

  return " ".join(filter(lambda x: x, (f"{properties['value']} {value_part}{'s' if properties['value'] > 1 else ''}" if properties.get("value", 0) > 0 else '' for value_part, properties in reversed(divisor.items()))))

# COMMAND ----------

# MAGIC %md
# MAGIC ###convert_bytes_in_label

# COMMAND ----------

def convert_bytes_in_label(bytes : float) -> str:
  """
  Parameters
  ----------
  bytes : float
    Value to convert.
  
  Returns
  str
    A label in more readble format of a big number
  """
  return convert_float_to_label(bytes, {
      "byte": {"standard_value": 1024}, 
      "kilobyte": {"standard_value": 1024},
      "megabyte": {"standard_value": 1024},
      "gigabyte": {"standard_value": 1024}
    }
  )

# COMMAND ----------

# MAGIC %md
# MAGIC #Classes

# COMMAND ----------

# MAGIC %md
# MAGIC ##Entity

# COMMAND ----------


class Entity:
  """
  Class to represent an entity in datalake
  """
  
  def __init__(self, **kwargs):
    """
    
    Init method of Entity class.

    Parameters
    ----------
    **kwargs : dict
      Options to add to the object. Some are manadatory : functional_domain, functional_sub_domain, data_type, data_source, entity, ingestion_type, sub_entity

    """
    self.env = os.environ["CATALOG"]
    self._mandatory_options = ["functional_domain", "functional_sub_domain", "data_type", "data_source", "entity", "ingestion_type", "sub_entity"]
    self.add_options(**kwargs)

  def _get_paths(self):
    """
    Save the raw, bronze and silver paths for future refrence
    """
    not_set_options = []
    
    for option in self._mandatory_options:
      if getattr(self, option, None) is None:
        not_set_options.append(option)
    if len(not_set_options) > 0:
      raise Exception(f"Options {not_set_options} is mandatory. Use the <self>.add_options(option1=value, option2=value, ...) method to set it")
    
    
    self.raw_volume_path = f"/Volumes/{self.env}_{self.functional_domain}/{self.functional_sub_domain}_raw_{self.data_type}_{self.data_source}_{self.entity}/{self.ingestion_type}/{self.sub_entity}/"
    self.bronze_schema_name = f"{self.env}_{self.functional_domain}.{self.functional_sub_domain}_bronze_{self.data_type}_{self.data_source}_{self.entity}.{self.sub_entity}"
    self.silver_schema_name = f"{self.env}_{self.functional_domain}.{self.functional_sub_domain}_silver_{self.data_type}_{self.entity}.{self.sub_entity}"

    return self
  def add_options(self, **kwargs : Dict[str, any]):
    """
    Method to add options to the entity

    Parameters
    ----------
    **kwargs : dict
      Any options to be added to the class
    
    Returns
    -------
    Self
      The class itself
    """
    already_exists = []
    for key, value in kwargs.items():
      if getattr(self, key, None) is not None:
        already_exists.append(key)
      setattr(self, key, value)
    if len(already_exists) > 0:
      print(f"Options [{','.join(already_exists)}] already exist and was updated.")
    return self
  
  def get_raw_files(self, additional_path : str = "") -> pyspark.sql.dataframe.DataFrame:
    """
    Method to get all the files in the raw volume

    Parameters
    ----------
    additional_path : str, default ""
      Additional path to add to the raw volume path
    
    Returns
    -------
    pyspark.sql.dataframe.DataFrame
      A Spark DataFrame with all the files in the raw volume
    """   
    self._get_paths()

    path = self.raw_volume_path + additional_path
    schema = StructType([
      StructField('path', StringType(), True), 
      StructField('name', StringType(), True), 
      StructField('size', LongType(), True), 
      StructField('modificationTime', LongType(), True)
    ])
    return spark.createDataFrame([f for f in dbutils.fs.ls(path) if f.isFile()], schema=schema)
  
  
  @staticmethod
  def doMerge(
    df_source : pyspark.sql.dataframe.DataFrame, 
    target_table_name : str, 
    primary_keys : Union[pyspark.sql.types.StructType, None] = None,
    merge_condition : Union[str, None] = None,
    operation_type_column : Union[str, None] = None
    ):
    """
    Perform a merge operation on a Delta table.

    Parameters
    ----------
    df_source : pyspark.sql.dataframe.DataFrame
      The source dataframe to merge.
    target_table_name : str
      The name of the target table.
    primary_keys : Union[pyspark.sql.types.StructType, None], default None
      The primary keys of the target table. If no merge_condition is provided, this parameter is mandatory.
    merge_condition : Union[str, None], default None
      The merge condition to use. If no primary keys are provided, this parameter is mandatory.
    operation_type_column : Union[str, None]
      The operation type column.

    Returns
    -------
    None
    """

    if primary_keys is None and merge_condition is None:
      raise Exception("Either primary_keys or merge_condition must be set")
    
    merge_condition = merge_condition if merge_condition is not None else " AND ".join(f'target_table.{key} = source_table.{key}' for key in primary_keys.names)
      # CURRENT CLUSTER CAN NOT EXECUTE THIS CODE BELOW
      # delta_table = DeltaTable.forPath(spark, target_table_name)
      #   if operation_type_column is not None:
      #     delta_table.alias("target").merge(
      #       df_source.alias("source"),
      #       primary_keys.names
      #     ).whenMatchedDelete(
      #       (F.col(operation_type_column) == 'D')
      #     ).whenMatchedUpdateAll(
      #       (F.col(operation_type_column) == 'I') | (F.col(operation_type_column).isNull())
      #     ).whenNotMatchedInsertAll(
      #       (F.col(operation_type_column) == 'I') | (F.col(operation_type_column).isNull())
      #     ).execute()
      #   else:
      #     delta_table.alias("target").merge(
      #       df_source.alias("source"),
      #       primary_keys.name
      #     ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    try:
      df_source.createOrReplaceTempView("source_table")
      if operation_type_column is not None:
        spark.sql(f"""
          MERGE INTO {target_table_name} AS target_table 
          USING source_table AS source_table
          ON  {merge_condition}
          WHEN MATCHED AND source_table.{operation_type_column} = 'D' THEN DELETE
          WHEN MATCHED AND ( source_table.{operation_type_column} = 'I' OR source_table.{operation_type_column} IS NULL) THEN UPDATE SET * 
          WHEN NOT MATCHED THEN INSERT * 
        """)
      else:
        spark.sql(f"""
          MERGE INTO {target_table_name} AS target_table 
          USING bronze AS source_table
          ON  {merge_condition}
          WHEN MATCHED THEN UPDATE SET * 
          WHEN NOT MATCHED THEN INSERT * 
        """)
    except Exception as e:
        df_target = spark.read.table(target_table_name)
        output = df_target.alias("target_table").join(
          df_source.alias("source_table"),
          how="outer",
          on=merge_condition
        ).select(
          [F.when(F.col(f"source_table.{col}").isNotNull(), F.col(f"source_table.{col}")).otherwise(F.col(f"target_table.{col}")).alias(col) for col in target_table_name]
        ).alias('output')

        if operation_type_column is not None: 
          output_table = output.where(F.col(operation_type_column) != 'D').alias('output_table')
        else:
          output_table = output.alias('output_table')

        output_table.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(target_table_name)

  def raw2Bronze(
    self, 
    raw_schema : pyspark.sql.types.StructType, 
    primary_keys : pyspark.sql.types.StructType, 
    additional_path : Union[Dict[str, str], None] = None,
    fatal_error_list : List[str] = [],
    non_fatal_error_list : List[str] = [DataFrameEmptyError, DataFrameDuplicateFoundError],
    write_mode : str = "append",
    process_file : str = "multiple",
    file_name : Union[str] = None,
    sequence_number_column : Union[str, None] = None, 
    operation_type_column : Union[str, None] = None, 
    bronze_schema : Union[pyspark.sql.types.StructType, None] = None, 
    delimiter : str ="|", 
    header : bool = True, 
    bronze_func : Union[any, None] = None, 
    f_args : Union[List[any], list] = [], 
    f_kwargs : Union[Dict[str, any], dict] = {}, 
    read_kwargs : Union[Dict[str, any], dict] = {}
    ) -> None:
    """
    Method to create the bronze table from the raw volumes and move tha raw csv file to the archive folder or error folder.

    Parameters
    ----------
    raw_schema : pyspark.sql.types.StructType
      Schema of the raw volumes as struct type of pyspark
    primary_keys : pyspark.sql.types.StructType
      Primary keys of the bronze table as struct type of pyspark
    additional_path : Union[Dict[str, str], None], default None
      When raw files are stored in different folder than the standard (example: historical load), you can use this parameter to override the default path.
      The dict must follow this schema:{
        "additional_raw_volume_path": <value>,
        "additional_archive_path": <value>,
        "additional_error_path": <value>
      }
    fatal_error_list : List[str], default []
      List of error that stop the execution of the job.
    non_fatal_error_list : List[str], default []
      List of error that stop the processing of the file.
    write_mode : str, default 'append'
      Either 'merge' or 'overwrite' or 'append'. 
      If 'merge', perform an 'UPSERT'. 
      If 'overwrite', perform a "TRUNCATE" and "INSERT".
      if 'append', perform an "INSERT".
    process_file : str, default 'multiple'
      Either 'multiple' or 'last' or 'single'. 
      If 'multiple', process all the files in the raw volume folder. 
      If 'last', process only the file in the raw volume folder that is the most recent.
      if 'single' process only one file in the raw volume folder. Must indicate the file name in the 'file_name' parameter.
    file_name : Union[str, None], default None
      Name of the file to process. Only used if process_file = 'single'.
    sequence_number_column : Union[str, None]
      Column to use in the window function. if None, no partition is performed
    operation_type_column : Union[str, None]
      Column to use when performing an 'UPSERT' to perform a delete.
    bronze_schema : pyspark.sql.types.StructType, optional
      Schema of the bronze table as struct type of pyspark, by default None. If None, the schema of the raw volumes will be used
    delimiter : str, optional
      Delimiter of the raw volumes, by default "|"
    header : bool, optional
      Whether the raw volumes have a header, by default True
    bronze_func : function, optional
      Transform function to apply on the raw volumes to create the bronze table, by default None. If passed and add some extra columns, the bronze_schema must be passed too.
    f_args : list, optional
      Arguments to be passed to the bronze_func, by default []
    f_kwargs : dict, optional
      Keyword arguments to be passed to the bronze_func, by default {}
    read_kwargs : dict
      Any other options to be passed to the read options method of pyspark

    Returns
    -------
    None
    """
    print(f"{get_local_time()} - SubEntity[{self.sub_entity}] - Start")
    self._get_paths()

    self.raw_schema = raw_schema
    self.primary_keys = primary_keys
    self.bronze_schema = bronze_schema or raw_schema
    self.sequence_number_column = sequence_number_column

    if additional_path is not None:
      raw_complete_path = self.raw_volume_path + additional_path.get("additional_raw_volume_path", "")
      raw_archive_path = self.raw_volume_path + additional_path.get("additional_archive_path", "")
      raw_error_path = self.raw_volume_path + additional_path.get("additional_error_path", "")
    else:
      raw_complete_path = self.raw_volume_path
      raw_archive_path = f"{self.raw_volume_path}archive/"
      raw_error_path = f"{self.raw_volume_path}error/"


    time_tables = []

    raw_files = [{"name": file.name, "path": file.path, "size_in_bytes": file.size} for file in dbutils.fs.ls(raw_complete_path) if file.isFile()]

    if len(raw_files) == 0:
      print(f"{get_local_time()} - SubEntity[{self.sub_entity}] - No files in the raw volume folder. Exiting...")
      return self
    
    if bronze_func is not None:
        if bronze_schema is None:
          raise ValueError("bronze_schema must be passed if bronze_func is passed")
    
    if process_file in ('last', 'single'):
      if process_file == 'single':
        if file_name is None:
          raise ValueError("file_name must be passed if process_file = 'single'")
        else:
          raw_files = [file for file in raw_files if file['name'] == file_name]
      elif process_file == 'last':
        raw_files = [raw_files[-1]]

    for file in raw_files:
      file_str_header = f"SubEntity[{self.sub_entity}] - Process file {file['name']}"
      print( f"{get_local_time()} - {file_str_header} - {convert_bytes_in_label(file['size_in_bytes'])}")
      try:
        start_time = get_local_time(return_type="datetime")
        df_raw = spark.read.options(delimiter=delimiter, header=header, **read_kwargs).schema(self.raw_schema).csv(file["path"])
        time_tables.append({ "activity": "read", "file_name": file["name"], "file_size": file["size_in_bytes"], "start_time": start_time, "end_time": get_local_time(return_type="datetime")})
        data_quality_check(df_raw, primary_keys, sequence_number_column, file["name"])
        time_tables.append({ "activity": "data_quality", "file_name": file["name"], "file_size": file["size_in_bytes"], "start_time": start_time, "end_time": get_local_time(return_type="datetime")})
      # List Exceptions here
      except (
        DataFrameEmptyError, 
        DataFrameKeysNullError, 
        DataFrameDuplicateFoundError, 
        DataFrameDuplicateRecordNumberWarning
      ) as e:
        ##send mail to someone ?
        if type(e) in fatal_error_list: 
          dbutils.fs.mv(file["path"], f"{raw_error_path}{file['name']}")
          print(f"{get_local_time()} - {file_str_header} - {type(e)}: {e} - Job will stop and file will be moved to error folder.")
          raise
        if type(e) in non_fatal_error_list: 
          dbutils.fs.mv(file["path"], f"{raw_error_path}{file['name']}")
          print(f"{get_local_time()} - {file_str_header} - {type(e)}: {e} - Job will continue but file will be moved to error folder.")
          continue
        print(f"{get_local_time()} - {file_str_header} - {type(e)}: {e} - Job will continue and also the execution of file.")
      except Exception as e:
        print(f"{get_local_time()} - {file_str_header} - {type(e)}: {e} - Job will continue but file will be moved to error folder.")
        dbutils.fs.mv(file["path"], f"{raw_error_path}{file['name']}")
        ##send mail to someone
        continue
      finally:
        time_tables.append({ "activity": "data_quality", "file_name": file["name"], "file_size": file["size_in_bytes"], "start_time": start_time, "end_time": get_local_time(return_type="datetime")})
      
      if bronze_func is not None:
        file_name = file["name"].replace(".csv", "")
        df_bronze = bronze_func(df_raw, file_name, *f_args, **f_kwargs)
        if sorted(df_bronze.schema.names) != sorted(bronze_schema.names):
          print(f"{get_local_time()} - {file_str_header} - bronze_schema target passed is different from the ouput schema from file and function.")
      else:
        df_bronze = df_raw.alias("bronze")
      
      if sequence_number_column is not None:
        df_bronze = df_bronze.withColumn(
          "last_row_key",
          F.row_number().over(Window.partitionBy(*primary_keys.names).orderBy(F.desc(sequence_number_column)))
        ).where(F.col("last_row_key") == 1).drop("last_row_key")


      df_bronze = df_bronze.dropna(how="any", subset=primary_keys.names)

      print(f"{get_local_time()} - {file_str_header} - writing {self.bronze_schema_name} with mode: {write_mode}")
      start_time = get_local_time(return_type="datetime")
      if write_mode == 'merge':
        self.doMerge(df_bronze, self.bronze_schema_name, primary_keys, operation_type_column=operation_type_column)
      else:
        df_bronze.write.mode(write_mode).format('delta').option('MergeSchema', 'true').saveAsTable(self.bronze_schema_name)
      
      time_tables.append({ "activity": "write", "file_name": file["name"], "file_size": file["size_in_bytes"], "start_time": start_time, "end_time": get_local_time(return_type="datetime")})
      start_time = get_local_time(return_type="datetime")
      dbutils.fs.mv(file["path"], raw_archive_path)
      time_tables.append({ "activity": "archive", "file_name": file["name"], "file_size": file["size_in_bytes"], "start_time": start_time, "end_time": get_local_time(return_type="datetime")})

      schema = StructType([
        StructField("activity", StringType(), False),
        StructField("file_name", StringType(), False),
        StructField("file_size", LongType(), False),
        StructField("start_time", TimestampType(), False),
        StructField("end_time", TimestampType(), False)
      ])
      
      self.df_time = spark.createDataFrame(time_tables, schema=schema)
      self.df_time.createOrReplaceTempView(f"df_time_{self.sub_entity}")

    print(f"{get_local_time()} - SubEntity[{self.sub_entity}] - End")
    return self
  
  def checkSilverTableExist(self, df_bronze : pyspark.sql.dataframe.DataFrame, operation_type_column : Union[str, None] = None) -> bool:
    """
    Method to check if the silver table exists. If not create from df_bronze

    Parameters
    ----------
    df_bronze : pyspark.sql.dataframe.DataFrame
      DataFrame of the bronze table
    
    Returns
    -------
    bool
      True if the silver table exists, False otherwise
    """
    try:
      spark.read.table(self.silver_schema_name)
    except AnalysisException as e:
      if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        print(f"SubEntity[{self.sub_entity}] - silver layer [{self.silver_schema_name}] does not exist. Creating it from bronze layer [{self.bronze_schema_name}]")
        if operation_type_column is not None: 
          output_silver = df_bronze.where(f.col(operation_type_column) != 'D').alias('output_silver')
        else:
          output_silver = df_bronze.alias('output_silver')
        df_bronze.write.mode('overwrite').format('delta').option('MergeSchema', 'true').saveAsTable(self.silver_schema_name)
      return False
    return True
  
  def bronze2Silver(
    self, 
    primary_keys : Union[pyspark.sql.types.StructType, None] = None, 
    write_mode : str = "merge", 
    operation_type_column : Union[str, None] = None, 
    sequence_number_column : Union[str, None] = None, 
    silver_schema : Union[pyspark.sql.types.StructType, None] = None,
    silver_func : Union[any, None] = None, 
    f_args : Union[List[any], list] = [], 
    f_kwargs : Union[Dict[str, any], dict] = {}
    ) -> None:
    """
    Method to promote the bronze layer to silver layer

    Parameters
    ----------
    primary_keys : pyspark.sql.types.StructType, default None
      Primary keys of the silver table in StructType of pyspark. If not passed, uses the primary_keys passed in the raw2bronze method
    write_mode : str, default 'merge'
      Either 'merge' or 'overwrite' or 'append'. 
      If 'merge', perform an 'UPSERT'. 
      If 'overwrite', perform a "TRUNCATE" and "INSERT".
      if 'append', perform an "INSERT".
    operation_type_column : Union[str, None], optional
      Column to use in the merge operation. If None, all rows will be merged
    sequence_number_column : Union[str, None]
      Column to use in the window function. if None, no partition is performed
    silver_schema : pyspark.sql.types.StructType, optional
      Schema of the silver table as struct type of pyspark, if not passed try to use the bronze_schema
    silver_func : function, optional
      Transform function to apply on the bronze table to create the silver table, by default None. If passed and add some extra columns, the silver_schema must be passed too.
    f_args : list, optional
      Arguments to be passed to the silver_func, by default []
    f_kwargs : dict, optional
      Keyword arguments to be passed to the silver_func, by default {}

    Returns
    -------
    None
    """
    self._get_paths()

    silver_schema = silver_schema or getattr(self, "bronze_schema", None)
    primary_keys = primary_keys or getattr(self, "primary_keys", None)
    sequence_number_column = sequence_number_column or getattr(self, "sequence_number_column", None)

    if primary_keys is None:
      raise Exception("primary_keys is mandatory if not set in the raw2bronze method")
    if silver_schema is None:
      raise Exception("silver_schema is mandatory if bronze_schema is not set.")
    if primary_keys is None:
      raise Exception("primary_keys is mandatory if not set in the raw2bronze method")
    if silver_func is not None:
        if silver_schema is None:
          raise ValueError("silver_schema must be passed if silver_func is passed.")

    self.silver_schema = silver_schema

    silver_schema_no_keys = StructType([field for field in self.silver_schema if field.name not in primary_keys.names])
    df_bronze = spark.read.table(self.bronze_schema_name)

    if sequence_number_column is not None:
      df_bronze = df_bronze.withColumn(
        "last_row_key",
        f.row_number().over(Window.partitionBy(*primary_keys.names).orderBy(F.desc(sequence_number_column)))
      ).where(f.col("last_row_key") == 1).drop("last_row_key")

    if silver_func is not None:
      df_bronze = silver_func(df_bronze, *f_args, **f_kwargs).alias("bronze")
      if sorted(df_bronze.schema.names) != sorted(silver_schema.names):
        print(f"SubEntity[{self.sub_entity}] - silver_schema target passed is different from the ouput schema from bronze table and silver function.")
    else:
      df_bronze = df_bronze.alias("bronze")

    print(f"SubEntity[{self.sub_entity}] - start write in {self.silver_schema_name} with {write_mode}")

    if self.checkSilverTableExist(df_bronze, operation_type_column):
      if write_mode == 'merge':
        self.doMerge(df_bronze, self.silver_schema_name, primary_keys, operation_type_column=operation_type_column)

      elif write_mode in ('overwrite', 'append'):
        ### ingestion with append or overwrite ###
        df_bronze.write.mode(write_mode).format("delta").option("mergeSchema", "true").saveAsTable(self.silver_schema_name)
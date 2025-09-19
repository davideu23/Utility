# Databricks notebook source
from typing import List

import pyspark

from pyspark.sql import functions as f

promotion_function_definition = {}

# COMMAND ----------

# MAGIC %md 
# MAGIC #Finance

# COMMAND ----------

# MAGIC %md
# MAGIC ##Accounting

# COMMAND ----------

# MAGIC %md
# MAGIC ###Copa

# COMMAND ----------

# MAGIC %md
# MAGIC ####1_co_pa100fe01_13_b

# COMMAND ----------

functional_domain, functional_subdomain, entity, sub_entity = 'finance', 'copa', 'copa', '1_co_pa100fe01_13_b'
promotion_function_definition.setdefault(functional_domain, {}).setdefault(functional_subdomain, {}).setdefault(entity, {}).setdefault(sub_entity, {})

def bronze_func(df_raw: pyspark.sql.dataframe.DataFrame, file_name: str, sub_entity: str, f_source: str) -> pyspark.sql.dataframe.DataFrame:
    """
    PySpark function used to enrich raw layer before promoting to bronze. This function is then passed to the method raw2bronze for class Entity. See the method for more details.

    Parameters
    ----------
    df_raw : pyspark.sql.dataframe.DataFrame
        Raw dataframe to be enriched.
    file_name : str
        File name of the raw dataframe.
    sub_entity : str
        Sub entity of the raw dataframe.
    f_source : str
        datasource of the raw dataframe

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        The enriched dataframe
    """
    df_raw = df_raw.withColumn(
        "SOURCE", f.lit(f_source)
    ).withColumn(
        "BUSINESS_DATE", f.lit(file_name.replace(f"{sub_entity}_", "").replace(".csv", ""))
    )

    return df_raw.alias("raw_enriched")

def silver_func(df_bronze: pyspark.sql.dataframe.DataFrame, date_col: List[str]) -> pyspark.sql.dataframe.DataFrame:
    """
    PySpark function used to enrich bronze layer before promoting to silver. This function is then passed to the method bronze2silver for class Entity. See the method for more details.

    Parameters
    ----------
    df_bronze : pyspark.sql.dataframe.DataFrame
        Bronze dataframe to be enriched.
    date_col : List[str]
        List of columns to be converted to date.

    Returns
    ------
    pyspark.sql.dataframe.DataFrame
        The enriched dataframe
    """
    for col in date_col:
        df_bronze = df_bronze.withColumn(col, f.to_date(f.col(col), "yyyy.MM.dd"))

    df_bronze = df_bronze.withColumn("UpdateTimestamp", f.current_timestamp())
    return df_bronze.alias("bronze_enriched")

promotion_function_definition[functional_domain][functional_subdomain][entity][sub_entity]["bronze_func"] = bronze_func
promotion_function_definition[functional_domain][functional_subdomain][entity][sub_entity]["silver_func"] = silver_func

# COMMAND ----------

# MAGIC %md
# MAGIC ###General Ledger

# COMMAND ----------

# MAGIC %md
# MAGIC ####0FI_GL4

# COMMAND ----------

import datetime
import pytz

functional_domain, functional_subdomain, entity, sub_entity = 'finance', 'accounting', 'general_ledger', '0FI_GL_4'
promotion_function_definition.setdefault(functional_domain, {}).setdefault(functional_subdomain, {}).setdefault(entity, {}).setdefault(sub_entity, {})

def silver_func(df_bronze: pyspark.sql.dataframe.DataFrame, f_source: str) -> pyspark.sql.dataframe.DataFrame:
    """
    PySpark function used to enrich bronze layer before promoting to silver. This function is then passed to the method bronze2Silver for class Entity. See the method for more details.

    Parameters
    ----------
    df_raw : pyspark.sql.dataframe.DataFrame
        Raw dataframe to be enriched. This is a required parameter that is always passed to the function.
    f_source : str
        datasource of the raw dataframe. Custom parameter

    Return
    -------
    pyspark.sql.dataframe.DataFrame
        The enriched dataframe
    """
    timezone = pytz.timezone('Europe/Rome')
    date_time = datetime.datetime.now(timezone).strftime('%Y-%m-%d %H:%M')

    df_bronze = df_bronze.withColumn("SOURCE", f.lit(f_source)).withColumn("DATE_INSERT", f.lit(date_time))

    return df_bronze.alias("bronze_enriched")

promotion_function_definition[functional_domain][functional_subdomain][entity][sub_entity]["bronze_func"] = None
promotion_function_definition[functional_domain][functional_subdomain][entity][sub_entity]["silver_func"] = silver_func

# COMMAND ----------


# Databricks notebook source
from math import (
  floor
)

from typing import (
  Dict
)

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

if __name__ == "__main__":
  bytes_conversion = {
        "byte": {"standard_value": 1024}, 
        "kilobyte": {"standard_value": 1024},
        "megabyte": {"standard_value": 1024},
        "gigabyte": {"standard_value": 1024}
      }
  time_conversion = {
    "millisecond": {"standard_value": 1000},
    "second": {"standard_value": 60}, 
    "minute": {"standard_value": 60},
    "hour": {"standard_value": 24},
    "day": {"standard_value": float('inf')}
  }

  print(convert_float_to_label(7278420621, bytes_conversion))
  print(convert_float_to_label(500000 * 1000, time_conversion))
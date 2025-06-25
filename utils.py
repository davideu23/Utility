import datetime, pytz

from typing import (
  Union
)

from math import (
  floor
)

def get_local_time(typ : str = "str", frmt : str = "%Y-%m-%d %H:%M:%S") ->Union[str, datetime.datetime]:
  """

  Parameters
  ----------
  typ : str, default "str"
    The type of the output. Either "str" or "datetime".
  frmt : str, default "%Y-%m-%d %H:%M:%S"
    The format of the output. Only used when typ is "str".

  Returns
  -------
  str or datetime.datetime
    The local time in the timezone of Rome.
  """
  valid_typ = ["str", "datetime"]
  if typ == "datetime":
    return datetime.datetime.now(tz=pytz.timezone("Europe/Rome"))
  if typ == "str":
    return datetime.datetime.now(tz=pytz.timezone("Europe/Rome")).strftime(frmt)
  raise ValueError(f"'typ' must be either one of {valid_typ}")





def convert_numeric_time_to_str(t : float) -> str:
  """

  Parameters
  ----------
  t : float
    Time in second.

  Returns
  -------
  str
    Time as string label
  """
  divisor = {
    "second": {"standard_time": 60}, 
    "minute": {"standard_time": 60},
    "hour": {"standard_time": 24},
    "day": {"standard_time": float('inf')}}


  for time_part, properties in divisor.items():
    if t >= properties["standard_time"]:
      current_time = t / properties["standard_time"]
      properties["value"] = int((current_time - floor(current_time)) * properties["standard_time"])
      t = (t - properties["value"]) / properties["standard_time"]
    else: 
      properties["value"] = int(t)
      break

  return " ".join(filter(lambda x: x, (f"{properties['value']} {time_part}{'s' if properties['value'] > 1 else ''}" if properties.get("value", 0) > 0 else '' for time_part, properties in reversed(divisor.items()))))
import importlib
from typing import Union, Optional
import os

from pathlib import Path
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
import yaml

from .config import get_config


config = get_config()
types = importlib.import_module('pyspark.sql.types', 'types')

schema_path = os.path.join(Path(__file__).parents[1], 'config.yaml')
# with open('../schema.yaml', 'r') as f:
with open(schema_path, 'r') as f:
    schema_cfg = yaml.safe_load(f)


def get_spark_session(
        n_executors: Union[str, int] = '*',
        session_name: str = 'data_cleaning'
) -> "pyspark.sql.SparkSession":

    spark = SparkSession.builder \
        .master(f"local[{n_executors}]") \
        .appName(f'{session_name}') \
        .getOrCreate()
    return spark.conf.set('spark.sql.repl.eagerEval.enabled', True)


def get_schema_dict():
    schema = {}
    for col, type_alias in schema_cfg['schema'].items():
        schema[col] = getattr(types, schema_cfg['types'][type_alias])

    return schema


def get_schema(types: dict, default: Optional["pyspark.sql.types"] = None) -> "pyspark.sql.types.StructType":
    if default is not None:
        return StructType([StructField(col, default) for col in types])
    else:
        return StructType([StructField(col, col_type()) for col, col_type in types.items()])
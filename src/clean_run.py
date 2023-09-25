from pyspark.sql.types import StringType
from pyspark.sql import functions as F

from utils import get_schema, get_schema_dict
from utils import get_spark_session
from utils import get_new_file_paths
from utils.config import get_config
from cleaning import DataCleaner


config = get_config()

files = get_new_file_paths(config)

if len(files) > 0:
    files = ', '.join(files)

    schema = get_schema_dict()
    schema_str = get_schema(schema, default=StringType())

    spark = get_spark_session()
    df = spark.read.csv(files, header=True, schema=schema_str)

    cleaner = DataCleaner(df, spark, schema, config, train=False)
    df = cleaner.clean()

    df.withColumn('month_year',
                  F.array_join(F.array(F.year('tx_datetime'), F.month('tx_datetime')), '-')) \
      .write.partitionBy('month_year')\
      .mode('overwrite')\
      .parquet(config['cleaned_data_s3'])

    spark.stop()


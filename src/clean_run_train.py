import os
import pickle

from pyspark.sql.types import StringType
from pyspark.sql import functions as F

from utils import get_schema, get_schema_dict
from utils import get_spark_session
from utils import save_artifacts_s3
from utils.config import get_config
from cleaning import DataCleaner


config = get_config()

schema = get_schema_dict()
schema_str = get_schema(schema, default=StringType())

spark = get_spark_session()
df = spark.read.csv(config['raw_data_s3'], header=True, schema=schema_str)

cleaner = DataCleaner(df, spark, schema, config, train=True)

# Получаем еще и артефакты для заполнения пропущенных значений в последующем
df, stats_per_user, stats_all_users = cleaner.clean()

df.withColumn('month_year',
              F.array_join(
                F.array(
                  F.year('tx_datetime'), F.month('tx_datetime')
                ), '-')) \
  .write.partitionBy('month_year') \
  .mode('overwrite') \
  .parquet(config['cleaned_data_s3'])

# Сохраняем артефакты в s3
path_s3 = config['paths']['artifacts_s3']
stats_per_user.write.mode('overwrite').parquet(os.path.join(path_s3, config['artifacts']['stats_per_user']))

path_local = 'stats_all_users.pkl'
with open(path_local, 'wb') as f:
    pickle.dump(stats_all_users, f)

save_artifacts_s3(stats_all_users,
                  path_local=path_local,
                  path_s3=os.path.join(path_s3, config['artifacts']['stats_all_users']))

os.remove(path_local)

spark.stop()


# df.withColumn('year', F.year('tx_datetime')) \
#   .withColumn('month', F.month('tx_datetime')) \
#   .write\
#   .partitionBy(['year', 'month']) \
#   .mode('overwrite')\
#   .parquet(config['cleaned_data_s3'])



import os
import pickle
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql import Window

from ..utils import load_artifacts_s3


def cast_columns(df: "pyspark.sql.DataFrame", schema_dict: dict) -> "pyspark.sql.DataFrame":
    return df.select([col(c).cast(schema_dict[c]) for c in df.columns])


class DataCleaner:
    def __init__(self, df: "DataFrame",
                 spark_session: "pyspark.sql.SparkSession",
                 schema: dict,
                 config: dict,
                 train: bool = False):
        self._df = df
        self._spark = spark_session
        self._train_flag = train
        self._config = config
        self._schema = schema

    @property
    def df(self):
        return self._df

    @property
    def train_flag(self):
        return self._train_flag

    @property
    def config(self):
        return self._config

    @property
    def schema(self):
        return self._schema

    def _cast_columns(self, df: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
        df = df.select([col(c).cast(self.schema[c]) for c in df.columns])
        return df

    @staticmethod
    def _parse_date_and_time(self, df: "DataFrame") -> "DataFrame":
        df = df.withColumn('datetime_splits', F.split('tx_datetime', ' ')) \
            .withColumn('date', col('datetime_splits').getItem(0)) \
            .withColumn('time', col('datetime_splits').getItem(1)) \
            .drop('datetime_splits')
        return df

    @staticmethod
    def _parse_hours_minutes_seconds(self, df: "DataFrame") -> "DataFrame":
        df = df.withColumn('time_splits', F.split('time', ':')) \
            .withColumn('hours', col('time_splits').getItem(0).cast('int')) \
            .withColumn('minutes', col('time_splits').getItem(1).cast('int')) \
            .withColumn('seconds', col('time_splits').getItem(2).cast('int')) \
            .drop('time_splits')
        return df

    @staticmethod
    def _correct_hours_and_date(self, df: "DataFrame") -> "DataFrame":
        df = df.withColumn('tmp', col('hours') > 23) \
            .withColumn('date', F.when(col('tmp'), F.date_add(col('date'), 1)).otherwise(col('date'))) \
            .withColumn('hours', F.when(col('tmp'), col('hours') - 24).otherwise(col('hours'))) \
            .drop('tmp')
        return df

    @staticmethod
    def _correct_minutes_and_hour(self, df: "DataFrame") -> "DataFrame":
        df = df.withColumn('tmp', col('minutes') > 59) \
            .withColumn('hours', F.when(col('tmp'), col('hours') + 1).otherwise(col('hours'))) \
            .withColumn('minutes', F.when(col('tmp'), col('minutes') - 60).otherwise(col('minutes'))) \
            .drop('tmp')
        return df

    @staticmethod
    def _correct_seconds_and_minutes(self, df: "DataFrame") -> "DataFrame":
        df = df.withColumn('tmp', col('seconds') > 59) \
            .withColumn('minutes', F.when(col('tmp'), col('minutes') + 1).otherwise(col('minutes'))) \
            .withColumn('seconds', F.when(col('tmp'), col('seconds') - 60).otherwise(col('seconds'))) \
            .drop('tmp')
        return df

    @staticmethod
    def _update_tx_datetime(self, df: "DataFrame") -> "DataFrame":
        df = df.withColumn('time', F.array_join(F.array('hours', 'minutes', 'seconds'), ':')) \
            .withColumn('tx_datetime', F.array_join(F.array('date', 'time'), ' ')) \
            .withColumn('tx_datetime', F.to_timestamp(col('tx_datetime'))) \
            .drop('hours', 'minutes', 'seconds', 'date', 'time')
        return df

    def _clean_datetime(self, df: "DataFrame") -> "DataFrame":
        df = df.transform(self._parse_date_and_time) \
            .transform(self._parse_hours_minutes_seconds) \
            .withColumn('date', F.to_date('date')) \
            .transform(self._correct_hours_and_date) \
            .transform(self._correct_minutes_and_hour) \
            .transform(self._correct_seconds_and_minutes) \
            .transform(self._update_tx_datetime)
        return df

    @staticmethod
    def _terminal_per_user(df: "DataFrame") -> "DataFrame":
        df_terminal_counts = df.dropna(subset=['customer_id', 'terminal_id']) \
                               .groupby('customer_id', 'terminal_id').count()

        df_terminal_max = df_terminal_counts.groupby('customer_id')\
                                            .agg(F.max('count').alias('max_freq'))
        df_terminal_counts = df_terminal_counts.join(df_terminal_max, on='customer_id')

        window = Window.partitionBy('customer_id').orderBy('terminal_id')

        mask = (F.col('count') == F.col('max_freq')) & (F.col('max_freq') > 1)
        df_most_freq_terminals = (
            df_terminal_counts.where(mask)
                              .withColumn('row', F.row_number().over(window))
                              .filter(F.col('row') == 1)
                              .drop('row')
                              .dropna()
        )
        return df_most_freq_terminals.select('customer_id', 'terminal_id')

    @staticmethod
    def _terminal_all_user(df: "DataFrame") -> float:
        most_freq_terminal = df.dropna(subset=['terminal_id']) \
                               .groupby('terminal_id') \
                               .count() \
                               .orderBy('count', ascending=False) \
                               .first()['terminal_id']
        return most_freq_terminal

    @staticmethod
    def _amount_per_user(df: "DataFrame") -> "DataFrame":
        amount_statistics = (
            df.dropna(subset=['customer_id'])
              .dropna(subset=['tx_amount'])
              .groupBy("customer_id")
              .agg(F.mean('tx_amount').alias('average_amount'),
                   F.expr('percentile_approx(tx_amount, 0.5, 10000)').alias('median_amount'))
        )
        return amount_statistics

    @staticmethod
    def _amount_all_user(df: "DataFrame") -> float:
        mean_val = df.dropna(subset=['tx_amount']) \
                     .select(F.mean('tx_amount').alias('average_amount')) \
                     .collect()[0]['average_amount']
        median_val = df.approxQuantile('tx_amount', [0.5], 0.01)[0]
        return mean_val, median_val

    def _get_stats_per_user(self, df):
        stats_per_user = self._amount_per_user(df).join(self._calc_most_freq_terminal(df),
                                                        on='customer_id', how='outer')
        # stats_per_user.write.mode('overwrite').parquet(self.config['features_to_fill'])
        return stats_per_user

    def _get_stats_all_user(self, df):
        stats_all_users = {}
        stats_all_users['most_freq_terminal'] = self._terminal_all_user(df)

        mean_val, median_val = self._amount_all_user(df)
        stats_all_users['average_amount'] = mean_val
        stats_all_users['median_amount'] = median_val

        return stats_all_users

    @staticmethod
    def _fillna(df: "DataFrame", stats_per_user: "DataFrame", stats_all_users: dict) -> "DataFrame":

        init_cols = df.columns

        wrong_terminal_mask = (
                F.isnan('terminal_id')
                | col('terminal_id').isNull()
                | (col('terminal_id') < 0)
        )

        wrong_amount_mask = (
                F.isnan('tx_amount')
                | col('tx_amount').isNull()
                | (col('tx_amount') <= 0)
        )

        df = df.join(stats_per_user.withColumnRenamed('terminal_id', 'most_freq_terminal'), on='customer_id', how='left')

        df = df.withColumn('terminal_id',
                           F.when(wrong_terminal_mask, col('most_freq_terminal')).otherwise(col('terminal_id')))\
               .withColumn('terminal_id',
                           F.when(wrong_terminal_mask, stats_all_users['most_freq_terminal']).otherwise(col('terminal_id')))\
               .withColumn('tx_amount',
                           F.when(wrong_amount_mask, col('median_amount')).otherwise(col('tx_amount')))\
               .withColumn('tx_amount',
                           F.when(wrong_amount_mask, stats_all_users['median_amount']).otherwise(col('tx_amount')))
        return df.select(init_cols)

    def clean(self):
        self.df = self._cast_columns(self.df)

        if self.train_flag:
            self.df = self.df.drop_duplicates() \
                          .dropna(subset=['tx_datetime', 'customer_id'])

            if 'tx_fraud' in self.df.columns:
                self.df = self.df.dropna(subset=['tx_fraud'])

            if 'tx_fraud_scenario' in self.df.columns:
                self.df = self.df.dropna(subset=['tx_fraud_scenario'])

        self.df = self._clean_datetime(self.df)
        self.df = self.df.withColumn(
            'tx_amount',
            F.when(F.col('tx_amount') <= 0.0, float('nan')).otherwise(F.col('tx_amount')).cast('float')
        )

        stats_per_user = self._get_stats_per_user(self.df)
        stats_all_users = self._get_stats_all_user(self.df)

        # сначала заполняем пропуски актуальными данными (без обращения к артифактам)
        self.df = self._fillna(self.df, stats_per_user, stats_all_users)

        # if self.train_flag:
        #     stats_per_user.write.mode('overwrite').parquet(self.config['features_to_fill'])
        #
        #     with open('stats_all_users.pkl', 'wb') as f:
        #         pickle.dump(stats_all_users, f)
        # else:
        #     stats_per_user_hist = self.spark.read.parquet(self.config['features_to_fill'])
        #
        #     # TODO: здесь нужно было бы из s3 качать
        #     with open('stats_all_users.pkl', 'rb') as f:
        #         stats_all_users_hist = pickle.load(f)
        #
        #     # затем заполняем пропуски из статистиками из трейна
        #     self.df = self._fillna(self.df, stats_per_user_hist, stats_all_users_hist)

        # return self.df

        # TODO: не забыть все логировать
        if not self.train_flag:
            stats_per_user_hist = self.spark.read.parquet(
                os.path.join(self.config['paths']['artifacts_s3'], self.config['artifacts']['stats_per_user'])
            )

            stats_all_users_hist = load_artifacts_s3(
                os.path.join(self.config['paths']['artifacts_s3'], self.config['artifacts']['stats_all_users'])
            )

            self.df = self._fillna(self.df, stats_per_user_hist, stats_all_users_hist)

            return self.df

        else:
            return self.df, stats_per_user, stats_all_users








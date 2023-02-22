from datetime import timedelta
from typing import List

from pyspark.sql.functions import col, max, min, avg, count
from pyspark.sql.dataframe import DataFrame


def df_division_by_company(df: DataFrame) -> List[DataFrame]:
    filtered_companies_list = list()
    companies_names = df.select(df.company).distinct().rdd.flatMap(lambda x: x).collect()

    for company_name in companies_names:
        filtered_df = df.select('*').where(df.company == company_name)
        filtered_companies_list.append(filtered_df)
        filtered_df.show()
    return filtered_companies_list


def df_division_by_time(df: DataFrame, interval: int) -> List[DataFrame]:
    dfs_list = list()
    time_interval_start = df.first().asDict().get('date')

    while True:
        time_interval_end = time_interval_start + timedelta(minutes=(interval - 1), seconds=59)
        temp_df = df.where(col('date').between(time_interval_start, time_interval_end))
        time_interval_start += timedelta(minutes=interval)

        if temp_df.count() < 3:
            break
        dfs_list.append(temp_df)
    return dfs_list


def get_statistic_data(df: DataFrame) -> DataFrame:
    return df.select(
        max(df.company).alias('company'),
        count(df.quote).alias('records_amount'),
        min(df.quote).alias("min_price"),
        max(df.quote).alias("max_price"),
        avg(df.quote).alias('avg_price'),
        min(df.date).alias('interval start'),
        max(df.date).alias('interval end'))

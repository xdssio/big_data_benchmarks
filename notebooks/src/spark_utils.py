from pyspark import sql, SparkConf, SparkContext
import pyspark.sql.functions as f
import numpy as np

conf = SparkConf().setAppName('Benchmarks')
conf.setExecutorEnv('spark.executor.memory', '2g')
conf.setExecutorEnv('spark.driver.memory', '30g')
sc = SparkContext(conf = conf)
sqlContext = sql.SQLContext(sc)


def read_file(df=None, data_path=None):
    return sqlContext.read.parquet(data_path)
    
def mean(df):
    return df.select(f.mean('fare_amount')).collect()
    
def standard_deviation(df):
    return df.select(f.stddev('fare_amount')).collect()

def sum_columns(df):
    return df.select(f.mean(df['fare_amount'] + df['passenger_count'])).collect()

def product_columns(df):
    return df.select(f.mean(df['fare_amount'] * df['passenger_count'])).collect()

def complicated_arithmetic_operation(df):
    theta_1 = df['pickup_longitude']
    phi_1 = df['pickup_latitude']
    theta_2 = df['dropoff_longitude']
    phi_2 = df['dropoff_latitude']

    temp = (f.cos(theta_1)*np.pi/180) * (f.cos(theta_2)*np.pi/180) * (f.sin(phi_2-phi_1)/2*np.pi/180)**2
    expression = 2 * f.atan2(f.sqrt(temp), f.sqrt(1-temp))
    df.select(f.mean(expression)).collect()


def value_counts(df):
    return df.select('fare_amount').distinct().collect()

def groupby_statistics(df):
    ret = df.groupby('pickup_hour').agg(
        f.mean('fare_amount'),
        f.stddev('fare_amount'),
        f.mean('tip_amount'),
        f.stddev('tip_amount')
    )
    ret.take(3)
    return ret

def join(df, other):
    ret = df.join(other, on = 'pickup_hour')
    ret.take(3)
    return ret
    

def filter_data(df):
    long_min = -74.05
    long_max = -73.75
    lat_min = 40.58
    lat_max = 40.90

    expr_filter = (df.pickup_longitude > long_min)  & (df.pickup_longitude < long_max) & \
              (df.pickup_latitude > lat_min)    & (df.pickup_latitude < lat_max) & \
              (df.dropoff_longitude > long_min) & (df.dropoff_longitude < long_max) & \
              (df.dropoff_latitude > lat_min)   & (df.dropoff_latitude < lat_max)
    ret = df[expr_filter]
    ret.take(3) # evaluate the filter
    return ret

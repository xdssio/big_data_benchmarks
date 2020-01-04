from pyspark import sql, SparkConf, SparkContext
import pyspark.sql.functions as f
import numpy as np

conf = SparkConf().setAppName('Read_CSV')
sc = SparkContext(conf = conf)
sqlContext = sql.SQLContext(sc)


def read_file(df=None, data_path=None):
    return sqlContext.read.csv(data_path, sep = ',', header = 'True', inferSchema = 'true')
    
def mean(df):
    return df.select(f.mean('fare_amount')).collect()
    
def standard_deviation(df):
    return df.select(f.stddev('fare_amount')).collect()

def sum_columns(df):
    return df.select(df['fare_amount'] + df['passenger_count']).collect()

def product_columns(df):
    return df.select(df['fare_amount'] * df['passenger_count']).collect() 

def complicated_arithmetic_operation(df):
    theta_1 = df['pickup_longitude']
    phi_1 = df['pickup_latitude']
    theta_2 = df['dropoff_longitude']
    phi_2 = df['dropoff_latitude']
    temp = ((np.cos(df.select(theta_1).collect())*np.pi/180)*np.cos(df.select(theta_2).collect())*np.pi/180) \
            * (np.sin((df.select(phi_2-phi_1).collect()))/2*np.pi/180)**2

    return 2 * np.arctan2(np.sqrt(temp), np.sqrt(1-temp))

def value_counts(df):
    return df.select('fare_amount').distinct().collect()

def groupby_statistics(df):
    return df.groupby('pickup_hour').agg(
        f.mean('fare_amount'),
        f.stddev('fare_amount'),
        f.mean('tip_amount'),
        f.stddev('tip_amount'))

def join(df, other):
    return df.join(other, on = 'pickup_hour')
    

def filter_data(df):
    long_min = -74.05
    long_max = -73.75
    lat_min = 40.58
    lat_max = 40.90

    expr_filter = (df.pickup_longitude > long_min)  & (df.pickup_longitude < long_max) & \
              (df.pickup_latitude > lat_min)    & (df.pickup_latitude < lat_max) & \
              (df.dropoff_longitude > long_min) & (df.dropoff_longitude < long_max) & \
              (df.dropoff_latitude > lat_min)   & (df.dropoff_latitude < lat_max)
    return df.filter(expr_filter)

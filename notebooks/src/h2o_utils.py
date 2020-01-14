import numpy as np
import h2o

h2o.init()

def read_file(data=None, data_path=None):
    return h2o.import_file(data_path)
    
def mean(df):
    return df['fare_amount'].mean()
    
def standard_deviation(df):
    return df['fare_amount'].sd()

def sum_columns(df):
    return df['fare_amount'] + df['trip_distance']

def product_columns(df):
    return df['fare_amount'] * df['trip_distance']

def complicated_arithmetic_operation(df):
    theta_1 = df['pickup_longitude'].as_data_frame().as_matrix()
    phi_1 = df['pickup_latitude'].as_data_frame().as_matrix()
    theta_2 = df['dropoff_longitude'].as_data_frame().as_matrix()
    phi_2 = df['dropoff_latitude'].as_data_frame().as_matrix()
    temp = (np.sin((theta_2-theta_1)/2*np.pi/180)**2
           + np.cos(theta_1*np.pi/180)*np.cos(theta_2*np.pi/180) * np.sin((phi_2-phi_1)/2*np.pi/180)**2)
    distance = 2 * np.arctan2(np.sqrt(temp), np.sqrt(1-temp))
    return distance * 3958.8

def value_counts(df):
    return df['passenger_count'].table()

def groupby_statistics(df):
    df_grouped = df.group_by(by = ['passenger_count'])
    df_grouped.mean(col = ['fare_amount', 'tip_amount']).sd(col = ['fare_amount', 'tip_amount'])
    return df_grouped.get_frame()
                                 
def join(df, other):
    return df.merge(other, by='passenger_count')
    

def filter_data(df):
    long_min = -74.05
    long_max = -73.75
    lat_min = 40.58
    lat_max = 40.90

    expr_filter = (df['pickup_longitude'] > long_min) & (df['pickup_longitude'] < long_max) & \
              (df['pickup_latitude']> lat_min) & (df['pickup_latitude'] < lat_max) & \
              (df['dropoff_longitude']> long_min) & (df['dropoff_longitude'] < long_max) & \
              (df['dropoff_latitude'] > lat_min) & (df['dropoff_latitude'] < lat_max)
    return df[expr_filter]

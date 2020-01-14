import pandas as pd
import numpy as np

def read_file(df=None, data_path=None):
    return pd.read_csv(data_path)
    
def mean(df):
    return df.fare_amount.mean()
    
def standard_deviation(df):
    return df.fare_amount.std()

def sum_columns(df):
    return df.eval('fare_amount + trip_distance') 

def product_columns(df):
    return df.eval('fare_amount * trip_distance') 

def complicated_arithmetic_operation(df):
    theta_1 = df.pickup_longitude
    phi_1 = df.pickup_latitude
    theta_2 = df.dropoff_longitude
    phi_2 = df.dropoff_latitude
    temp = (np.sin((theta_2-theta_1)/2*np.pi/180)**2
           + np.cos(theta_1*np.pi/180)*np.cos(theta_2*np.pi/180) * np.sin((phi_2-phi_1)/2*np.pi/180)**2)
    return 2 * np.arctan2(np.sqrt(temp), np.sqrt(1-temp))

def value_counts(df):
    return df.passenger_count.value_counts()

def groupby_statistics(df):
    return df.groupby(by='passenger_count').agg({'fare_amount': ['mean', 'std'], 
                                               'tip_amount': ['mean', 'std']
                                              })
def join(df, other):
    return df.join(other=other, on = 'passenger_count', rsuffix = '_right')
    

def filter_data(df):
    long_min = -74.05
    long_max = -73.75
    lat_min = 40.58
    lat_max = 40.90

    expr_filter = (df.pickup_longitude > long_min)  & (df.pickup_longitude < long_max) & \
                  (df.pickup_latitude > lat_min)    & (df.pickup_latitude < lat_max) & \
                  (df.dropoff_longitude > long_min) & (df.dropoff_longitude < long_max) & \
                  (df.dropoff_latitude > lat_min)   & (df.dropoff_latitude < lat_max)
    return df[expr_filter]

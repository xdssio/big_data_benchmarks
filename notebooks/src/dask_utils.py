import dask.dataframe as dd
import numpy as np


def read_file_parquet(df=None, data_path=None):
    return dd.read_parquet(data_path, engine='pyarrow')

def read_file_vaex(df=None, data_path=None, chunksize='auto', column_names=None):
    import vaex
    data_path = '/data/yellow_taxi_2009_2015_f32.hdf5'
    df = vdf = vaex.open(data_path)
    column_names = column_names or vdf.get_column_names()
    ddf = dd.from_array(vdf[column_names].to_dask_array(chunks=chunksize), columns=column_names)
    # we manually add this column of a different type in
    ddf['passenger_count'] = vdf['passenger_count'].to_dask_array(chunksize)
    return ddf
    
def mean(df):
    return df.fare_amount.mean().compute()
    
def standard_deviation(df):
    return df.fare_amount.std().compute()

def mean_of_sum(df):
    return (df.fare_amount + df.trip_distance).mean().compute()

def mean_of_product(df):
    return (df.fare_amount * df.trip_distance).mean().compute()

def mean_of_complicated_arithmetic_operation(df):
    theta_1 = df.pickup_longitude
    phi_1 = df.pickup_latitude
    theta_2 = df.dropoff_longitude
    phi_2 = df.dropoff_latitude
    temp = (np.sin((theta_2-theta_1)/2*np.pi/180)**2
           + np.cos(theta_1*np.pi/180)*np.cos(theta_2*np.pi/180) * np.sin((phi_2-phi_1)/2*np.pi/180)**2)
    ret = 2 * np.arctan2(np.sqrt(temp), np.sqrt(1-temp))
    return ret.mean().compute()

def value_counts(df):
    return df.fare_amount.value_counts().compute()

def groupby_statistics(df):
    return df.groupby(by='passenger_count').agg({'fare_amount': ['mean', 'std'], 
                                               'tip_amount': ['mean', 'std']
                                              }).compute()
def join(df, other):
    return dd.merge(df, other, left_index=True, right_index=True).compute()
#     return df.join(other=other, on = 'pickup_hour', rsuffix = '_right')
    

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

def length(df):
    return len(df)

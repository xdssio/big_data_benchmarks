import vaex
import numpy as np
import datatable as dt
from datatable import f, math

def read_file(data=None, data_path=None):
    vdf = vaex.open(data_path)
    columns = {}
    for name in vdf.get_column_names():
        data = vdf.columns[name]
        if data.dtype == str:
            pass  # skip strings
        elif data.dtype.kind == 'f':
            # datatable is picky about <f4 format
            columns[name] = data.view(np.float32)
        elif data.dtype.kind == 'i':
            columns[name] = data
        else:
            pass  # ignore non int and float
    return dt.Frame(**columns)
    

def mean(df):
    return df[:, dt.mean(dt.f.fare_amount)]
    

def standard_deviation(df):
    return df[:, dt.sd(dt.f.fare_amount)]


def mean_of_sum(df):
    return df[:, dt.mean(f.fare_amount + f.trip_distance)]


def mean_of_product(df):
    return df[:, dt.mean(f.fare_amount * f.trip_distance)]


def complicated_arithmetic_operation(df):
    theta_1 = f.pickup_longitude
    phi_1 = f.pickup_latitude
    theta_2 = f.dropoff_longitude
    phi_2 = f.dropoff_latitude
    temp = (math.sin((theta_2-theta_1)/2*math.pi/180)**2
           + math.cos(theta_1*math.pi/180)*math.cos(theta_2*math.pi/180) * math.sin((phi_2-phi_1)/2*math.pi/180)**2)
    expr = 2 * math.atan2(math.sqrt(temp), math.sqrt(1-temp))
    return df[:, dt.mean(expr)]


def value_counts(df):
    return df['passenger_count'].value_counts()


def groupby_statistics(df):
    aggs = {
            'fare_amount_mean': dt.mean(f.fare_amount),
            'fare_amount_std': dt.sd(f.fare_amount),
            'tip_amount_mean': dt.mean(f.tip_amount),
            'tip_amount_std': dt.sd(f.tip_amount),
        }
    return df[:, aggs, dt.by(f.passenger_count)]    

def join(df, other):
    # like vaex and dask, no precomputed index
    other.key = 'passenger_count'
    return df[:,:,dt.join(other)]


def filter_data(df):
    long_min = -74.05
    long_max = -73.75
    lat_min = 40.58
    lat_max = 40.90

    expr_filter = (f.pickup_longitude > long_min)  & (f.pickup_longitude < long_max) & \
              (f.pickup_latitude > lat_min)    & (f.pickup_latitude < lat_max) & \
              (f.dropoff_longitude > long_min) & (f.dropoff_longitude < long_max) & \
              (f.dropoff_latitude > lat_min)   & (f.dropoff_latitude < lat_max)
    return df[expr_filter,:]


def length(df):
    return len(df)
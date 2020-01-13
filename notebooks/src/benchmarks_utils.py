import pandas as pd
import numpy as np
import datetime as dt
import warnings
import time
import gc
import os

warnings.filterwarnings("ignore")
os.makedirs('../results', exist_ok=True)

def benchmark(f, df, repetitions=1, **kwargs):
    times = []
    for i in range(repetitions):
        start_time = time.time()
        ret = f(df, **kwargs)
        times.append(time.time()-start_time)
    return np.mean(times)

def get_results(benchmarks, name):
    results = pd.DataFrame.from_dict(benchmarks, orient='index')
    results.columns = [name]
    return results


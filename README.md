Big data techonlogy benchmarks
This project is designed to compare big data technologies comparisons for cleaning, manipulating and generally wrangling data in purpose of analysis and machine learning. 

Please read this [Blog](https://medium.com/p/a453a1f8cc13/edit) for more information.

The analysis is done on a [100GB Texi data 2009 - 2015](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

*Technologies*
* [x] [Pandas](https://pandas.pydata.org/)
* [x] [Vaex](https://github.com/vaexio/vaex)
* [x] [H2O](https://github.com/h2oai/h2o-3)
* [x] [Turicreate](https://github.com/apple/turicreate)
* [x] [Dask](https://github.com/dask/dask)
* [x] [PySpark](https://github.com/apache/spark)
* [x] [koalas](https://github.com/databricks/koalas)
* [x] [Modin](https://github.com/modin-project/modin)
* [x] [Datatable](https://github.com/h2oai/datatable)

## General Remarks
* Some notebooks requeire a restart of the karnel after package installation.
* Different notebooks run on different kernels, check out on the top what is what.
* The notebooks of technologies who don't run out of core are set to work with only 1M rows.
* On special cases notebooks needed to be restarted for optmial performance - that might not be fair, but I wanted to try to get the most out of each technology.

# Instructions
0. Create an S3 bucket to put your results (or remove this part in the *persist* function in the code).
1. Create a *ml.c5d.4xlarge* instance on [AWS SageMaker](https://aws.amazon.com/sagemaker/) with extra 500G Stroage.
2. Run the *get_data.ipynb* notebook to mount the SSD and download the data.
3. Run the notebook you want to test.
* In each notebook and the beginning, make sure the name of the instance and the S3 bucket is right. 

Good luck!

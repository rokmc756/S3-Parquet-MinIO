## What is s3-parquet-minio?
The purpose of this python code is to prepare sample data into minio for loading
parquet data files  from minio as a representitive of s3 storage to Greenplum via PXF

## Supported and confirmed OS versoins so far
Rocky Linux 9.x

## Prerequisite
#### The following python package and pip module should be installed for running this python code.
~~~
[root@rk9-minio ~ ]# yum install python3-pip
[root@rk9-minio ~ ]# pip install s3fs
[root@rk9-minio ~ ]# pip install pyarrow
[root@rk9-minio ~ ]# pip install pandas
[root@rk9-minio ~ ]# pip install python-dotenv
[root@rk9-minio ~ ]# pip install python-dotenv[cli]
Requirement already satisfied: python-dotenv[cli] in /usr/local/lib/python3.9/site-packages (1.0.0)
Collecting click>=5.0
  Downloading click-8.1.3-py3-none-any.whl (96 kB)
     |████████████████████████████████| 96 kB 1.7 MB/s
Installing collected packages: click
Successfully installed click-8.1.3
~~~

## Usage
#### Download from github and configure your s3 object storeage such as minio
~~~
$ git clone https://github.com/rokmc756/s3-parquet-minio
$ cd s3-parquet-minio

$ vi s3-config.env
S3_REGION="kr-central-1"
S3_ENDPOINT="http://rk9-minio.jtest.pivotal.io:9000"
S3_ACCESS_KEY="minioadmin"
S3_SECRET_KEY="changeme"
SAMPLE_DATA="sample-data/data.parquet"
~~~

#### Check how to use
~~~
$ ./s3-parquet.py -h
Usage: ./s3-parquet.py [ -r < read|write> ] [ -l <local|s3> ] [ -b < bucket_name > ] [ -f < file_name > ]
~~~
#### Write and read sample parquet file into local disk
~~~
$ ./s3-parquet.py -c write -l local
$ ./s3-parquet.py -c read -l local
~~~
#### Write and read sample parquet file into bucket of s3 object storage
~~~
$ ./s3-parquet.py -c write -l s3 -b jbucket01 -f data
$ ./s3-parquet.py -c read -l s3 -b jbucket01 -f data
~~~

## Reference
https://janakiev.com/blog/pandas-pyarrow-parquet-s3/
https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html

## TODO
- Delete parquet file from s3 object storage such as minio
- Check parquet file if there are already in the bucket when adding new parquet file
- Add/delete/read/check multiple parquet files from s3 object storage such as minio

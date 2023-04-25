#!/usr/bin/python
#
# https://janakiev.com/blog/pandas-pyarrow-parquet-s3/
# https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html

# The purpose of this python code is to prepare sample data into minio for loading
# parquet data files  from minio as a representitive of s3 storage to Greenplum via PXF
#
# 2023-04-25, VMWare Tanzu Support
# Staff Product Support Engineer, Jack Moon <moonja@vmware.com>

import os, sys, time, getopt, s3fs           #
import numpy as np                           #
import pandas as pd                          #
import pyarrow as pa                         #
import pyarrow.parquet as pq                 #
from pyarrow import Table                    #
from dotenv import load_dotenv               #
from dotenv import dotenv_values

load_dotenv('./s3-config.env');
# s3config = dotenv_values('./s3-config.env');

#
def connect_s3():
    fs = s3fs.S3FileSystem( anon=False, use_ssl=False,
        client_kwargs={
            "region_name": os.environ['S3_REGION'],
            "endpoint_url": os.environ['S3_ENDPOINT'],
            "aws_access_key_id": os.environ['S3_ACCESS_KEY'],
            "aws_secret_access_key": os.environ['S3_SECRET_KEY'],
            "verify": True,
        }
    )
    return fs

def read_parquet_local( _local_filepath ):
    pf = pq.ParquetDataset( _local_filepath )
    print("[ Read data ]")
    print(pf.read().to_pandas())
    # print(pf.metadata)  # Depreicated
    # print(pf.fragments)
    # print(pf.schema)

def write_parquet_local( _local_filepath ):
    df = pd.DataFrame({'data': np.random.random((1000,))})
    df.to_parquet( _local_filepath )

def write_parquet_s3( _bucket_name, _filename ):
    fs = connect_s3()
    df = pd.DataFrame({'data': np.random.random((1000,))})
    # df.to_parquet( _local_filepath )

    with fs.open(_bucket_name+'/'+_filename+'.parquet', 'wb') as f:
        df.to_parquet(f)

    s3_filepath = _bucket_name+'/'+_filename+'.parquet'

    pq.write_to_dataset(
        Table.from_pandas(df),
        s3_filepath,
        filesystem=fs,
        use_dictionary=True,
        compression="snappy",
        version="2.4",
    )

    read_parquet_s3( _bucket_name, _filename )

def read_parquet_s3( _bucket_name, _filename ):
    fs = connect_s3()
    s3_filepath = "s3://" + _bucket_name
    s3_filepaths = [path for path in fs.ls(s3_filepath)
       if path.endswith('.parquet')]

    print("[ Check parquet file location ]")
    print(s3_filepaths)
    print("")

    s3_filepath = _bucket_name + "/" + _filename + ".parquet"
    # s3_filepath = _bucket_name + "/" + _filename + ".parquet"
    pf = pq.ParquetDataset(s3_filepath,filesystem=fs)

    print("[ List the content of parquet file ]")
    print(pf.read().to_pandas())

    # dataset = pq.ParquetDataset('dataset_name/', use_legacy_dataset=False, filters=[('n_legs','=',4)])

def usage():
  print("Usage: ./s3-parquet.py [ -r < read|write> ] [ -l <local|s3> ] [ -b < bucket_name > ] [ -f < file_name > ]")
  return

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:],"c:f:l:b:i:o:",["usage"])
    except getopt.GetoptError as err:
        print(str(err))
        usage()
        sys.exit(1)

    for opt,arg in opts:
        if (opt == "-c"):
            _command=arg
        elif ( opt == "-l"):
            _location=arg
        elif ( opt == "-b"):
            _bucket=arg
        elif ( opt == "-f"):
            _files=arg
        elif ( opt == "-h") or ( opt == "--help"):
            usage()
        else:
            usage()


    # Call functions according to options given
    _sample_data=os.getenv("SAMPLE_DATA")

    if ( _command ) == "read" and ( _location == "local" ):
        read_parquet_local( _sample_data )
    elif ( _command ) == "write" and ( _location == "local" ):
        write_parquet_local( _sample_data )
    elif ( _command ) == "read" and ( _location == "s3" ) and ( _files != "" ):
        read_parquet_s3( _bucket, _files )
    elif ( _command ) == "write" and ( _location == "s3" ) and ( _files != "" ):
        write_parquet_s3( _bucket, _files )
    else:
        usage

    return

if __name__ == '__main__':
    main()


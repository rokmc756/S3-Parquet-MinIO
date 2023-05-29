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
from minio import Minio
from pyminio import Pyminio


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

#
def delete_parquet_s3(_bucket, _filename):

    fs = connect_s3()
    s3_filepath = "s3://" + _bucket
    s3_filepaths = [path for path in fs.ls(s3_filepath)
       if path.endswith('.parquet')]

    print("[ Check parquet file location ]")
    print(s3_filepaths)
    # print("")

    # exit(1)

    # s3_filepath = _bucket + "/" + _filename + ".parquet"
    s3_filepath = _bucket + "/" + _filename
    # pf = pq.ParquetDataset(s3_filepath,filesystem=fs)

    # print("[ List the content of parquet file ]")
    # print(pf.read().to_pandas())

    fs.rm(s3_filepaths)  # single file

    exit(1)

    if os.path.isdir(_filename):
        for _fn in os.listdir(_filename):
            with fs.open(_bucket_name+'/'+'/'+_filename+'/'+_fn, 'wb') as f:
                df.to_parquet(f)
            # s3_filepath = _bucket_name+'/'+_fn
            # s3_filepath = _bucket_name+'/'+_fn+'.parquet'
            s3_filepath = _bucket_name+'/'+_fn
            pq.write_to_dataset(Table.from_pandas(df),s3_filepath,filesystem=fs,use_dictionary=True,compression="snappy", version="2.4",)
            read_parquet_s3( _bucket_name, _fn )
 

    file_system.rm('s3://my-bucket/foo.txt')  # single file

    # files = ['s3://my-bucket/bar.txt', 's3://my-bucket/baz.txt']
    # file_system.rm(files)  # several files


    df = pd.DataFrame({'data': np.random.random((1000,))})

    if os.path.isdir(_filename):
        for _fn in os.listdir(_filename):
            with fs.open(_bucket_name+'/'+'/'+_filename+'/'+_fn, 'wb') as f:
                df.to_parquet(f)
            # s3_filepath = _bucket_name+'/'+_fn
            # s3_filepath = _bucket_name+'/'+_fn+'.parquet'
            s3_filepath = _bucket_name+'/'+_fn
            pq.write_to_dataset(Table.from_pandas(df),s3_filepath,filesystem=fs,use_dictionary=True,compression="snappy", version="2.4",)
            read_parquet_s3( _bucket_name, _fn )
    elif os.path.isfile(_filename):
        with fs.open(_bucket_name+'/'+'/'+_filename, 'wb') as f:
            df.to_parquet(f)
        # s3_filepath = _bucket_name+'/'+_filename+'.parquet'
        s3_filepath = _bucket_name+'/'+_filename
        pq.write_to_dataset(Table.from_pandas(df),s3_filepath,filesystem=fs,use_dictionary=True,compression="snappy", version="2.4",)
        read_parquet_s3( _bucket_name, _filename )
    else:
       usage()



#
def test_exists(self, dataset, mocked_csvs_in_s3):
    assert PartitionedDataSet(mocked_csvs_in_s3, dataset).exists()

    empty_folder = "/".join([mocked_csvs_in_s3, "empty", "folder"])
    assert not PartitionedDataSet(empty_folder, dataset).exists()

    s3fs.S3FileSystem().mkdir(empty_folder)
    assert not PartitionedDataSet(empty_folder, dataset).exists()

#
def test_protocol_usage(self, filepath, instance_type):
    data_set = CSVDataSet(filepath=filepath)
    assert isinstance(data_set._fs, instance_type)

    # _strip_protocol() doesn't strip http(s) protocol
    if data_set._protocol == "https":
        path = filepath.split("://")[-1]
    else:
        path = data_set._fs._strip_protocol(filepath)

#
def convert_parquet_to_csv( _local_filepath ):
    df = pd.read_parquet( _local_filepath )
    # print( _local_filepath.split('.')[0] + '.csv' )
    df.to_csv( _local_filepath.split('.')[0] + '.csv' )
    # df.to_csv('filename.csv')

#
def convert_csv_to_parquet( _local_filepath ):
    df = pd.read_csv( _local_filepath )
    df.to_parquet( _local_filepath.split('.')[0] + '-test-' + '.parquet' )

#
def read_parquet_local( _local_filepath ):
    pf = pq.ParquetDataset( _local_filepath )
    print("[ Read data ]")
    print(pf.read().to_pandas())
    # print(pf.metadata)  # Depreicated in v2
    # print(pf.fragments)
    # print(pf.schema)

#
def write_parquet_local( _local_filepath ):
    df = pd.DataFrame({'data': np.random.random((1000,))})
    df.to_parquet( _local_filepath )

#
def write_parquet_s3( _bucket_name, _filename ):
    fs = connect_s3()
    df = pd.DataFrame({'data': np.random.random((1000,))})

    if os.path.isdir(_filename):
        for _fn in os.listdir(_filename):
            with fs.open(_bucket_name+'/'+'/'+_filename+'/'+_fn, 'wb') as f:
                df.to_parquet(f)
            # s3_filepath = _bucket_name+'/'+_fn
            # s3_filepath = _bucket_name+'/'+_fn+'.parquet'
            s3_filepath = _bucket_name+'/'+_fn
            pq.write_to_dataset(Table.from_pandas(df),s3_filepath,filesystem=fs,use_dictionary=True,compression="snappy", version="2.4",)
            read_parquet_s3( _bucket_name, _fn )
    elif os.path.isfile(_filename):
        with fs.open(_bucket_name+'/'+'/'+_filename, 'wb') as f:
            df.to_parquet(f)
        # s3_filepath = _bucket_name+'/'+_filename+'.parquet'
        s3_filepath = _bucket_name+'/'+_filename
        pq.write_to_dataset(Table.from_pandas(df),s3_filepath,filesystem=fs,use_dictionary=True,compression="snappy", version="2.4",)
        read_parquet_s3( _bucket_name, _filename )
    else:
       usage()

#
def read_parquet_s3( _bucket_name, _filename ):
    fs = connect_s3()
    s3_filepath = "s3://" + _bucket_name
    s3_filepaths = [path for path in fs.ls(s3_filepath)
       if path.endswith('.parquet')]

    print("[ Check parquet file location ]")
    print(s3_filepaths)
    print("")

    # s3_filepath = _bucket_name + "/" + _filename + ".parquet"
    s3_filepath = _bucket_name + "/" + _filename
    pf = pq.ParquetDataset(s3_filepath,filesystem=fs)

    print("[ List the content of parquet file ]")
    print(pf.read().to_pandas())

    # dataset = pq.ParquetDataset('dataset_name/', use_legacy_dataset=False, filters=[('n_legs','=',4)])

#
def usage():
  print("Usage: ./s3-parquet.py [ -r < read|write> ] [ -l <local|s3> ] [ -b < bucket_name > ] [ -f < file_name > ] [ -d < directory_name > ]")
  return

#
def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:],"c:f:l:b:d:s:t:",["command",
                                                                      "file",
                                                                      "location",
                                                                      "bucket",
                                                                      "directory",
                                                                      "source",
                                                                      "target",
                                                                      "help",
                                                                      ])
    except getopt.GetoptError as err:
        print(str(err))
        usage()
        sys.exit(1)

#
    for opt,arg in opts:
        if opt in ('-c', '--command'):
            _command=arg
        elif opt in ('-l', '--location'):
            _location=arg
        elif opt in ('-b', '--bucket'):
            _bucket=arg
        elif opt in ('-d', '--directory'):
            _directory=arg
        elif opt in ('-f', '--file'):
            _file=arg
        elif opt in ('-s', '--source'):
            _source=arg
        elif opt in ('-t', '--target'):
            _target=arg
        elif opt in ('-h', '--help'):
            usage()
        else:
            usage()

    # Call functions according to options given
    _sample_data=os.getenv("SAMPLE_DATA")

#
    if ( _command  == "read" ) and ( _location == "local" ):
        read_parquet_local( _sample_data )
    elif ( _command  == "write" ) and ( _location == "local" ):
        write_parquet_local( _sample_data )
    elif ( _command == "convert" ) and ( _file != "" ) and ( _source == "parquet" ) and  ( _target  == "csv" ):
        convert_parquet_to_csv( _file )
    elif ( _command == "convert" ) and ( _file != "" ) and ( _source == "csv" ) and  ( _target  == "parquet" ):
        convert_csv_to_parquet( _file )
    elif ( _command == "write" ) and ( _location == "s3" ) and ( _directory is not None ):
        write_parquet_s3( _bucket, _directory )
    elif ( _command == "read" ) and ( _location == "s3" ) and ( _file is not None ):
        read_parquet_s3( _bucket, _file )
    elif ( _command == "delete" ) and ( _location == "s3" ) and ( _file is not None ):
        delete_parquet_s3( _bucket, _file )
    elif ( _command == "write" ) and ( _location == "s3" ) and ( _file is not None ):
        write_parquet_s3( _bucket, _file )
    else:
        usage()

    return

if __name__ == '__main__':
    main()


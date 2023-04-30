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
[root@rk9-minio ~ ]# pip install minio
[root@rk9-minio ~ ]# pip install pyminio

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

## Use s3cmd instead of this python code
~~~
$ yum install s3cmd -y

$ s3cmd --configure

Enter new values or accept defaults in brackets with Enter.
Refer to user manual for detailed description of all options.

Access key and Secret key are your identifiers for Amazon S3. Leave them empty for using the env variables.
Access Key [minioadmin]:
Secret Key [changeme]:
Default Region [eu-central-1]:

Use "s3.amazonaws.com" for S3 Endpoint and not modify it to the target Amazon S3.
S3 Endpoint [http://rk9-minio.jtest.pivotal.io:9000]: rk9-minio.jtest.pivotal.io:9000

Use "%(bucket)s.s3.amazonaws.com" to the target Amazon S3. "%(bucket)s" and "%(location)s" vars can be used
if the target S3 system supports dns based buckets.
DNS-style bucket+hostname:port template for accessing a bucket [No]:

Encryption password is used to protect your files from reading
by unauthorized persons while in transfer to S3
Encryption password [changeme]:
Path to GPG program [/usr/bin/gpg]:

When using secure HTTPS protocol all communication with Amazon S3
servers is protected from 3rd party eavesdropping. This method is
slower than plain HTTP, and can only be proxied with Python 2.7 or newer
Use HTTPS protocol [No]:

On some networks all internet access must go through a HTTP proxy.
Try setting it here if you can't connect to S3 directly
HTTP Proxy server name:

New settings:
  Access Key: minioadmin
  Secret Key: changeme
  Default Region: eu-central-1
  S3 Endpoint: rk9-minio.jtest.pivotal.io:9000
  DNS-style bucket+hostname:port template for accessing a bucket: No
  Encryption password: changeme
  Path to GPG program: /usr/bin/gpg
  Use HTTPS protocol: False
  HTTP Proxy server name:
  HTTP Proxy server port: 0

Test access with supplied credentials? [Y/n] y
Please wait, attempting to list all buckets...
Success. Your access key and secret key worked fine :-)

Now verifying that encryption works...
Success. Encryption and decryption worked fine :-)

Save settings? [y/N] y
Configuration saved to '/root/.s3cfg'


$ s3cmd --config ~/.s3cfg put parqute-sample-data/part-m-00000.gz.parquet  s3://jbucket01
WARNING: part-m-00000.gz.parquet: Owner username not known. Storing UID=502 instead.
upload: 'parqute-sample-data/part-m-00000.gz.parquet' -> 's3://jbucket01/part-m-00000.gz.parquet'  [1 of 1]
 235841 of 235841   100% in    0s    16.84 MB/s  done
~~~

## How to see metadata or schema and so on in parquet file
~~~
$ pip install parquet-cli          //installs via pip

# View the metadata
$ parq parqute-sample-data/part-m-00000.gz.parquet

 # Metadata
 <pyarrow._parquet.FileMetaData object at 0x7fa274647950>
  created_by: parquet-mr version 1.5.0 (build 79977453b8cd65e6244f16316fac3a510aa87aa8)
  num_columns: 17
  num_rows: 100000
  num_row_groups: 1
  format_version: 1.0
  serialized_size: 2088
terminate called without an active exception
Aborted (core dumped)

# View the schema
$ parq parqute-sample-data/part-m-00000.gz.parquet --schema

 # Schema
 <pyarrow._parquet.ParquetSchema object at 0x7f72270a7d00>
required group field_id=-1 pig_schema {
  optional binary field_id=-1 MONTH0 (String);
  optional binary field_id=-1 CUSTOMER_ID (String);
  optional binary field_id=-1 SERVICE (String);
  optional binary field_id=-1 SERVICE_NUMBER (String);
  optional binary field_id=-1 TYPE_OF_SHAREPLUS__MOBILE_ (String);
  optional binary field_id=-1 SHAREPLUS_PARENT_NUMBER__MOBILE_ (String);
  optional binary field_id=-1 STUDENT_DISCOUNT_COMPONENT__MOBILE_ (String);
  optional binary field_id=-1 CIS_STATUS___SUB__MOBILE_ (String);
  optional binary field_id=-1 HANDSET__MOBILE_ (String);
  optional binary field_id=-1 HUBCLUB_MEMBER (String);
  optional binary field_id=-1 GENDER (String);
  optional binary field_id=-1 RACE (String);
  optional binary field_id=-1 AGE_BAND (String);
  optional binary field_id=-1 RESIDENT_STATUS (String);
  optional binary field_id=-1 ADDRESS_ID___HH (String);
  optional binary field_id=-1 POSTAL_CODE___HH (String);
  optional binary field_id=-1 SMART_CARD_NUMBER (String);
}

terminate called without an active exception
Aborted (core dumped)


# View top 10 rows
$ parq parqute-sample-data/part-m-00000.gz.parquet --head 10
   MONTH0 CUSTOMER_ID SERVICE SERVICE_NUMBER TYPE_OF_SHAREPLUS__MOBILE_
0  201601    10000001     FBB      123456789                             \
1  201601    10000002     FBB      123456790
2  201601    10000003     FBB      123456791
3  201601    10000004     FBB      123456792
4  201601    10000005     FBB      123456793
5  201601    10000006     FBB      123456794
6  201601    10000007     FBB      123456795
7  201601    10000008     FBB      123456796
8  201601    10000009     FBB      123456797
9  201601    10000010     FBB      123456798

  SHAREPLUS_PARENT_NUMBER__MOBILE_ STUDENT_DISCOUNT_COMPONENT__MOBILE_
0                                                                       \
1
2
3
4
5
6
7
8
9

  CIS_STATUS___SUB__MOBILE_ HANDSET__MOBILE_ HUBCLUB_MEMBER GENDER     RACE
0                                                                M  Chinese  \
1                                                                F  Chinese
2                                                                M    Malay
3                                                                M   Indian
4                                                                M  Chinese
5                                                                F  Chinese
6                                                                F  Chinese
7                                                                F    Malay
8                                                                F    Malay
9                                                                F   Indian

  AGE_BAND RESIDENT_STATUS ADDRESS_ID___HH POSTAL_CODE___HH SMART_CARD_NUMBER
0    20-25        Resident             ABC           123111              None
1    25-30        Resident             ABC           123112              None
2    25-30        Resident             ABC           123113              None
3    40-45        Resident             ABC           123114              None
4    45-50        Resident             ABC           123115              None
5    20-25        Resident             ABC           123116              None
6    30-35    Non Resident             ABC           123117              None
7    30-35        Resident             ABC           123118              None
8    45-50        Resident             ABC           123119              None
9    30-35        Resident             ABC           123120              None
~~~

## TODO
- Delete parquet file from s3 object storage such as minio
- Check parquet file if there are already in the bucket when adding new parquet file
- Add/delete/read/check multiple parquet files from s3 object storage such as minio

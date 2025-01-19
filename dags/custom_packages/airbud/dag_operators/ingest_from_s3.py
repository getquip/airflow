# Import Third Party Libraries
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# get a list of all the files to be processed
# copy the files to gcs
# push the name of the files to xcom

# get the list of files from xcom
# move the files to processed folder
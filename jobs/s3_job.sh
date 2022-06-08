######################################################################################
#File: s3_job.sh
#Description: This shell script is created to create a bucket in S3, upload the python scripts.
# If you have more scripts then update the function do_create_s3_and_copy(). 
# Providing any other option other than CREATE in the $1 will TEARDOWN
######################################################################################

#!/bin/bash
do_create_s3_and_copy(){
    echo "Starting to create the s3 bucket and copying the required job files"
    local cbucket_name=$1
    local CODE_PATH=$2
    aws s3 mb s3://$cbucket_name --profile udacity-de-course
    aws s3 cp $CODE_PATH/dl.cfg s3://$cbucket_name/ --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers --profile udacity-de-course
    aws s3 cp $CODE_PATH s3://$cbucket_name/ --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers --recursive --exclude "*" --include "*.py" --profile udacity-de-course
    echo "Job process is now completed, viewing the s3 bucket"
    aws s3 ls s3://$cbucket_name --profile udacity-de-course
}

teardown_s3(){
    echo "Starting to remove the bucket forcefully with its contents"
    local cbucket_name=$1
    aws s3 rb s3://$cbucket_name --force --profile udacity-de-course
    if [ $? -eq 255 ]; then
        echo "The bucket you are deleting does not exist in AWS S3"
        exit 1
    else
        echo "Process is completed"
        exit 0
    fi
}

# Bucketname: emr-et-udacity13321
# Code Path: /Users/subashprakash/Udacity/Spark_Project/spark-etl-aws/
option=$1
bucket_name=$2
CODE_PATH=$3
if [ "$option" = "CREATE" ]; then
    do_create_s3_and_copy $bucket_name $CODE_PATH
else
    teardown_s3 $bucket_name
fi

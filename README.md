# Dynamodb to Bigquery
This codebase is designed to be deployed on AWS lambda function for which the triger would be one of the
dynamodb table. The sole purpose of this codebase is to allow data sync from dynamodb to google bigquery via awc lambda functions. Currently it handles only the INSERT(or create) element triggers from dynamodb but wil evolve to 
support deletion and updations.


## How to use this code base
1) You will have to do some standard lambda function processes to use this. 1) clone this repo.  2) prepare the 
lambda function package(as explained by AWS guidelines).  3) upload the package to aws lambda.
3) This code base requires 4 things to run.

    a) Google credential files. You can download the google credentials file from the google cloud console. You can put this credential file into the lambda function package while preparing it during deploy.
    
    b) Environment variable `GOOGLE_APPLICATION_CREDENTIALS` which contains the path to the google credential file.
    
    c) Environment variable `DATASET_ID`, dataset it on google big query.
    
    d) Environment variable `TABLE_NAME`, table name in which you want to store you data.
    


### Note:
    This does not support dynamic change in column's datatype as this is not supported by bigquery at the moment and needs manual intervention: https://cloud.google.com/bigquery/docs/managing-table-schemas

import json
import boto3
import csv
# import os
# import codecs
# import sys
# import pandas as pd

s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')

# bucket = os.environ['bucket']
# key = os.environ['key']
# tableName = os.environ['table']
# bucket = "nworksijyucsvbucketimport"
# key = "ijyucities_add.csv"
tableName = "DevIjyuRegion"


# def lambda_handler(event, context):
def lambda_handler():

    # # get() does not store in memory
    # try:
    #     obj = s3.Object(bucket, key).get()['Body']
    # except:
    #     print("S3 Object could not be opened. Check environment variable. ")
    # try:
    #     table = dynamodb.Table(tableName)
    # except:
    #     print("Error loading DynamoDB table. Check if table was created correctly and environment variable.")

    batch_size = 100
    batch = []

    csv_file_path = "./data/dev_ijyu_region.csv"
    # csv_file = open(csv_file_path, "r", encoding="ms932", errors="", newline="")
    # csv_file = open(csv_file_path, "r", encoding="utf-8", errors="", newline="")

    with open(csv_file_path, "r", encoding="utf-8", errors="", newline="") as f:
        # DictReader is a generator; not stored in memory
        for row in csv.DictReader(f):
            if len(batch) >= batch_size:
                write_to_dynamo(batch)
                batch.clear()

            batch.append(row)

    if batch:
        write_to_dynamo(batch)

    return {
        'statusCode': 200,
        'body': json.dumps('Uploaded to DynamoDB Table')
    }


def write_to_dynamo(rows):
    try:
        table = dynamodb.Table(tableName)
    except:
        print("Error loading DynamoDB table. Check if table was created correctly and environment variable.")

    try:
        with table.batch_writer() as batch:
            for i in range(len(rows)):
                # print("put_item")
                batch.put_item(
                    Item=rows[i]
                )
    except:
        print("Error executing batch_writer")


if __name__ == '__main__':
    # csv_file_path = "./data/dev_ijyu_region.csv"
    # key = [x for x in uploaded_file.keys()]
    # data = pd.read_csv(key[0])

    lambda_handler()

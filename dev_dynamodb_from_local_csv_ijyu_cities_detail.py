import json
import boto3
import csv
from boto3.session import Session

profile = 'default'
session = Session(profile_name=profile)

# dynamodb = boto3.resource('dynamodb')
dynamodb = session.resource('dynamodb')

tableName = "DevIjyuCitiesDetail"
csv_file_path = "data/dev_ijyu_cities_detail.csv"


def lambda_handler():

    batch_size = 100
    batch = []

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
    lambda_handler()

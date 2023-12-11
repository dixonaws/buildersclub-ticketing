import time

import boto3
import pprint
import json

from boto3.dynamodb.conditions import Key

print("Setting up SDK...", end="")
dynamodb_resource=boto3.resource('dynamodb')
michigan_stadium_table=dynamodb_resource.Table('michigan_stadium')
print("done.")

block_id=2

start_time=time.time()
print("Querying michigan_stadium table using block-index... ", end="")
response=michigan_stadium_table.query(IndexName='block-index', KeyConditionExpression=Key('block').eq(block_id))
end_time=time.time()
total_time=end_time-start_time
print("done (" + str(round(total_time, 1)) + " seconds)")

pp=pprint.PrettyPrinter(indent=4)
lst_response=response['Items']

print("Table query using block-index, returning all seats in block " + str(block_id))
print("Printing last three items...")
pp.pprint(lst_response[:3])

start_time=time.time()
print("Querying michigan_stadium table using block-row-index... ", end="")
response=michigan_stadium_table.query(IndexName='block-row-index', KeyConditionExpression=Key('block').eq(block_id) & Key('row').eq(10))
end_time=time.time()
total_time=end_time-start_time
print("done (" + str(round(total_time, 1)) + " seconds)")

pp=pprint.PrettyPrinter(indent=4)
lst_response=response['Items']

print("Table query using block-row-index, returning all seats in block " + str(block_id) + " and row 10")
print("Printing last three items...")
pp.pprint(lst_response[:3])

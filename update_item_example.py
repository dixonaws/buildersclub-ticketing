import time
from decimal import Decimal

import boto3
import pprint
import json

from boto3.dynamodb.conditions import Key

print("Setting up SDK...", end="")
dynamodb_resource = boto3.resource('dynamodb')
michigan_stadium_table = dynamodb_resource.Table('michigan_stadium')
dynamodb_client = boto3.client('dynamodb')
print("done.")

str_seatid = "b8f8cc66-4c7b-46da-9198-908484b84487"

start_time = time.time()
print("Querying michigan_stadium table for seat " + str_seatid, end="")
# works
# response = michigan_stadium_table.query(KeyConditionExpression=Key('seatid').eq(str_seatid))

# works
response = dynamodb_client.get_item(TableName='michigan_stadium', Key={
    'seatid': {'S': str_seatid},
    'block': {'N': '1'}
})

# does not work: need to supply the seatid and the block (which we would know as we "zoom in" to the detail
# response = dynamodb_client.get_item(TableName='michigan_stadium', Key={
#     'seatid': {'S': str_seatid}
#
# })

end_time = time.time()
total_time = end_time - start_time
print("done (" + str(round(total_time, 1)) + " seconds)")

pp = pprint.PrettyPrinter(indent=4)
print(response)

# update item
response = michigan_stadium_table.update_item(
    Key={
        'seatid': {'S': str_seatid},
        'block': {'N': '1'}
    },
    UpdateExpression="set #status=:s",
    ExpressionAttributeNames={"#status":"status"},
    ExpressionAttributeValues={":s": "SOLD"},
    ReturnValues="UPDATED_NEW"
)

print(response)

# read the item again
response = dynamodb_client.get_item(TableName='michigan_stadium', Key={
    'seatid': {'S': str_seatid},
    'block': {'N': '1'}
})

end_time = time.time()
total_time = end_time - start_time
print("done (" + str(round(total_time, 1)) + " seconds)")

pp = pprint.PrettyPrinter(indent=4)
print(response)
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from chalice import Chalice
import boto3

app = Chalice(app_name='get-seats')
dynamo_client = boto3.client('dynamodb')
dynamo_resource = boto3.resource('dynamodb')


@app.route('/')
def index():
    return {'hello': 'world'}

@app.route('/{str_tablename}/{str_block}', methods=['GET'])
def get_block(str_tablename, str_block):
    str_return_message = "table: " + str_tablename + ", block: " + str_block
    print("Getting: " + str_return_message)

    table = dynamo_resource.Table(str_tablename)

    try:
        response = table.query(IndexName='block-index',
                               KeyConditionExpression=Key('block').eq(int(str_block)))

    except ClientError as err:
        print(
            "Couldn't query for block %s. Here's why: %s: %s",
            str_block,
            err.response["Error"]["Code"],
            err.response["Error"]["Message"],
        )
        raise
    else:
        return response["Items"]

@app.route('/str_tablename}/{str_seatid', methods=['POST'])
def update_seat(str_tablename, str_seatid):
    pass


# The view function above will return {"hello": "world"}
# whenever you make an HTTP GET request to '/'.
#
# Here are a few more examples:
#
# @app.route('/hello/{name}')
# def hello_name(name):
#    # '/hello/james' -> {"hello": "james"}
#    return {'hello': name}
#
# @app.route('/users', methods=['POST'])
# def create_user():
#     # This is the JSON body the user sent in their POST request.
#     user_as_json = app.current_request.json_body
#     # We'll echo the json body back to the user in a 'user' key.
#     return {'user': user_as_json}
#
# See the README documentation for more examples.
#

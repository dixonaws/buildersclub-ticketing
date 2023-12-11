import sys

import boto3
import decimal
import json
import logging
import os
import pprint
import time
import boto3
from botocore.exceptions import ClientError

# create a stadium in DynamoDB where each item represents an individual seat
# the stadium has 50 sections, each with 200 rows
# each row has 10 seats
# sections are made up of blocks of 4000 seats (2 sections, for viewing in the interface)
# the total seating capacity is 100,000 (200*10=2000, 2000*50=100000)

logger = logging.getLogger(__name__)
dynamodb = boto3.resource("dynamodb")
dynamodb_client=boto3.client("dynamodb")

MAX_GET_SIZE = 100  # Amazon DynamoDB rejects a get batch larger than 100 items.

def delete_table_if_exists(str_table_name):
    lst_tables=dynamodb_client.list_tables()['TableNames']
    if str_table_name in lst_tables:
        print(str_table_name + " table exists!, deleting... ")
        tbl_table=dynamodb.Table(str_table_name)
        tbl_table.delete()
        tbl_table.wait_until_not_exists()

def create_table(table_name, schema):
    """
    Creates an Amazon DynamoDB table with the specified schema.

    :param table_name: The name of the table.
    :param schema: The schema of the table. The schema defines the format
                   of the keys that identify items in the table.
    :return: The newly created table.
    """
    delete_table_if_exists(table_name)

    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": item["name"], "KeyType": item["key_type"]}
                for item in schema
            ],
            AttributeDefinitions=[
                {"AttributeName": item["name"], "AttributeType": item["type"]}
                for item in schema
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
        )
        table.wait_until_exists()
        logger.info("Created table %s.", table.name)

    except ClientError:
        logger.exception("Couldn't create table.")
        raise
    else:
        return table

def do_batch_get(batch_keys):
    """
    Gets a batch of items from Amazon DynamoDB. Batches can contain keys from
    more than one table.

    When Amazon DynamoDB cannot process all items in a batch, a set of unprocessed
    keys is returned. This function uses an exponential backoff algorithm to retry
    getting the unprocessed keys until all are retrieved or the specified
    number of tries is reached.

    :param batch_keys: The set of keys to retrieve. A batch can contain at most 100
                       keys. Otherwise, Amazon DynamoDB returns an error.
    :return: The dictionary of retrieved items grouped under their respective
             table names.
    """
    tries = 0
    max_tries = 5
    sleepy_time = 1  # Start with 1 second of sleep, then exponentially increase.
    retrieved = {key: [] for key in batch_keys}
    while tries < max_tries:
        response = dynamodb.batch_get_item(RequestItems=batch_keys)
        # Collect any retrieved items and retry unprocessed keys.
        for key in response.get("Responses", []):
            retrieved[key] += response["Responses"][key]
        unprocessed = response["UnprocessedKeys"]
        if len(unprocessed) > 0:
            batch_keys = unprocessed
            unprocessed_count = sum(
                [len(batch_key["Keys"]) for batch_key in batch_keys.values()]
            )
            logger.info(
                "%s unprocessed keys returned. Sleep, then retry.", unprocessed_count
            )
            tries += 1
            if tries < max_tries:
                logger.info("Sleeping for %s seconds.", sleepy_time)
                time.sleep(sleepy_time)
                sleepy_time = min(sleepy_time * 2, 32)
        else:
            break

    return retrieved

# snippet-start:[python.example_code.dynamodb.PutItem_BatchWriter]
def fill_table(table, table_data):
    """
    Fills an Amazon DynamoDB table with the specified data, using the Boto3
    Table.batch_writer() function to put the items in the table.
    Inside the context manager, Table.batch_writer builds a list of
    requests. On exiting the context manager, Table.batch_writer starts sending
    batches of write requests to Amazon DynamoDB and automatically
    handles chunking, buffering, and retrying.

    :param table: The table to fill.
    :param table_data: The data to put in the table. Each item must contain at least
                       the keys required by the schema that was specified when the
                       table was created.
    """
    try:
        with table.batch_writer() as writer:
            for item in table_data:
                writer.put_item(Item=item)
        logger.info("Loaded data into table %s.", table.name)
    except ClientError:
        logger.exception("Couldn't load data into table %s.", table.name)
        raise

def get_batch_data(seat_table, seat_list):
    """
    Gets data from the specified movie and actor tables. Data is retrieved in batches.

    :param seat_table: The table from which to retrieve movie data.
    :param seat_list: A list of keys that identify movies to retrieve.
    :return: The dictionary of retrieved items grouped under the respective
             movie and actor table names.
    """
    batch_keys = {
        seat_table.name: {
            "Keys": [{"seatid": seat[0], "block": seat[1]} for seat in seat_list]
        }
    }
    try:
        retrieved = do_batch_get(batch_keys)
        for response_table, response_items in retrieved.items():
            logger.info("Got %s items from %s.", len(response_items), response_table)
    except ClientError:
        logger.exception(
            "Couldn't get items from %s and %s.", seat_table.name
        )
        raise
    else:
        return retrieved

def usage_demo(str_input_data_file, int_items_to_process):
    """
    Shows how to use the Amazon DynamoDB batch functions.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")



    print(f"Getting seat data from {str_input_data_file}... ", end="")
    try:
        with open(str_input_data_file) as json_input_data:
            seat_data = json.load(json_input_data, parse_float=decimal.Decimal)
            if(int_items_to_process==0): int_items_to_process=len(seat_data)
            print("Processing " + str(int_items_to_process) + " items...")
            seat_data = seat_data[:int_items_to_process]  # Only use the first int_items_to_process
    except FileNotFoundError:
        print(
            f"The file input file was not found in the current working directory "

        )
        return

    print("done.")
    seat_schema = [
        {"name": "seatid", "key_type": "HASH", "type": "S"},
        {"name": "block", "key_type": "RANGE", "type": "N"},
    ]

    print("Creating venue table... ", end="")
    seat_table = create_table((str_input_data_file.split("."))[0], seat_schema)
    print(f"done, created {seat_table.name}.")

    print(f"Putting {len(seat_data)} seats into {seat_table.name}.")
    fill_table(seat_table, seat_data)

    seat_list = [
        (seat["seatid"], seat["block"])
        for seat in seat_data[0 : int(MAX_GET_SIZE / 2)]
    ]

    items = get_batch_data(seat_table, seat_list)
    print(
        f"Got {len(items[seat_table.name])} seats from {seat_table.name}\n"

    )
    print("The first 2 seats returned are: ")
    pprint.pprint(items[seat_table.name][:2])

def syntax():
    print("buildersclub-ticketing load_data_dynamo")
    print("Load seat data into a DynamoDB table")
    print()
    print("Syntax: python load_data_dynamo.py [input_datafile.txt] [items_to_process")
    print()
    print("Required: input_datafile.txt represents the file to import into DynamoDB. The table name will be the same as the file name, without the extension.")
    print("Required: items_to_process represents the number of items to input into the the DynamoDB table. Specify 0 to input all items.")


if __name__ == "__main__":
    try:
        str_input_data_file=sys.argv[1]
        int_items_to_process=int(sys.argv[2])

        print("Using parameters:")
        print("Input data file: " + str_input_data_file)
        print("Items to process: " + str(int_items_to_process))
        usage_demo(str_input_data_file, int_items_to_process)
    except IndexError:
        syntax()







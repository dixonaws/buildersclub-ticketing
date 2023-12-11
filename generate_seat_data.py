import json
import uuid
import time
import random
from datetime import datetime
import pprint
import sys


# create a stadium in DynamoDB where each item represents an individual seat
# the stadium has 50 sections, each with 200 rows
# each row has 10 seats
# sections are made up of blocks of 250 seats (for viewing in the interface)
# the total seating capacity is 100,000 (200*10=20000, 20000*50=100000)

def write_data_file(lst_seats, str_data_file):
    file_data_file = open(str_data_file, "w")
    file_data_file.write(json.dumps(lst_seats))
    file_data_file.flush()
    file_data_file.close()

def generate_data(str_data_file):
    lst_seats = []

    dict_seat = {}
    int_block=1
    for int_section in range(1, 51):
        # print("Section: " + str(int_section)+ ", Block: " + str(int_block))

        # each block is 2 sections (200 rows of 10 seats each) = 4000 seats
        if (int_section % 2 == 0):
            int_block = int_block+1

        for int_row in range(1, 201):
            for int_seat in range(1, 11):
                dict_seat = {}
                dict_seat['seatid'] = str(uuid.uuid4())
                dict_seat['section'] = int(int_section)
                dict_seat['row'] = int(int_row)
                dict_seat['lastupdated'] = time.time()
                dict_seat['seat'] = int_seat
                dict_seat['orderid'] = ''
                dict_seat['status'] = random.choice(['OPEN', 'PENDING', 'SOLD'])
                dict_seat['block'] = int_block
                # print("Adding seat: " + dict_seat['seatid'] + ", section: " + str(int_section) + ", row: " + str(int_row) + ", seat: " + str(int_seat) + ", status: " + dict_seat['status'] + ", block: " + str(int_block))
                lst_seats.append(dict_seat)

    print(str(len(lst_seats)) + " seats in lst_seats.")

    print("Writing data file... ", end="")
    write_data_file(lst_seats, str_data_file)
    print("done.")

    print("First three entries in the data file:")
    pp=pprint.PrettyPrinter(indent=4)
    pp.pprint(lst_seats[:3])


"""
# reference seat entry
{
    "seatid": "afd7d2b0-a3a1-4de0-8b66-1422ca8014bc",
    "section": 1,
    "row": 1,
    "lastupdated": 1701951713.912107,
    "seat": 1,
    "orderid": "",
    "status": "open",
    "block": 1

}"""

def syntax():
    print("buildersclub-ticketing generate_seat_data")
    print("Creates a JSON file of 100,000 seats that represent a stadium")
    print()
    print("Syntax: python generate_seat_data.py [output_datafile.txt]")
    print()
    print("output_datafile.txt represents the file to write")


if __name__ == '__main__':
    try:
        str_data_file=sys.argv[1]
        print("Using parameters:")
        print("Data file: " + str_data_file)
        generate_data(str_data_file)
    except IndexError:
        syntax()

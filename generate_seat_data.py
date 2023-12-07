import uuid
import time
import random

# create a stadium in DynamoDB where each item represents an individual seat
# the stadium has 50 sections, each with 200 rows
# each row has 10 seats
# sections are made up of blocks of 250 seats (for viewing in the interface)
# the total seating capacity is 100,000 (200*10=20000, 20000*50=100000)


def generate_data():
    lst_seats = []

    dict_seat = {}

    for int_section in range(1,51):
        for int_row in range(1, 201):
            for int_seat in range(1, 11):
                int_block=1
                print("section: " + str(int_section) + ", row: " + str(int_row) + ", seat: " + str(int_seat))
                dict_seat['seatid'] = str(uuid.uuid4())
                dict_seat['section'] = int(int_section)
                dict_seat['row'] = int(int_row)
                dict_seat['lastupdated'] = time.time()
                dict_seat['seat']=int_seat
                dict_seat['orderid']=''
                dict_seat['status']=random.choice(['OPEN', 'PENDING', 'SOLD'])
                dict_seat['block']=int_block
                lst_seats.append(dict_seat)

    print(str(len(lst_seats)) + " seats in lst_seats.")
    print(lst_seats.sort(key=lambda x: x['row']))

"""
{
    "seatid": "afd7d2b0-a3a1-4de0-8b66-1422ca8014bc",
    "section": 1,
    "row": 1,
    "lastupdated": 1701951713.912107,
    "seat": 1,
    "orderid": "",
    "status": "open",
    "block": 1

},"""

if __name__ == '__main__':
    generate_data()

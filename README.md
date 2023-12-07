buildersclub-ticketing

This program creates a representation of a stadium in DynamoDB, where each item represents an individual seat

The sample data stadium is meant to represent a high-capacity venue, like Michigan Stadium in Ann Arbor, MI
It has 50 sections, each with 200 rows
Each row has 10 seats
Sections are made up of blocks of 250 seats (for viewing in the interface)
The total seating capacity is 100,000 (200*100=20000, 2000*50=100000)

Dynamo Batching example: https://github.com/awsdocs/aws-doc-sdk-examples/tree/main/python/example_code/dynamodb/batching




import pandas as pd
import json
import random
import time
import hashlib
import os
from datetime import timedelta
import boto3

# Import necessary modules and libraries
from utils_events import get_second_event, get_coords, create_masive_users

# Import custom functions from the 'utils_events' module}
# Define Kinesis stream details and create a Kinesis client
stream_name = 'stream-Cloud9KinesisAthena'
region = 'us-east-2'
KinesisClient = boto3.client('kinesis', region_name=region)

# List of cities in Guatemala
cities = ['Guatemala', 'Progreso', 'Peten', 'Izabal', 'Sacatepequez']
# List of online payment methods
payment_online = ['Credit_card', 'PSE', 'Paypal']
# List of operating systems
operating_systems = ['ANDROID', 'IOS', 'WEB']  # Changed 'os' variable name to 'operating_systems'
# Initial event when launching the app
initial_event = 'LAUNCH_APP'
# Sequence of events for the second stage
second_event = ['HOME', 'EXIT_APP', 'HOME']
# Sequence of events for the third stage
third_event = ['GO_TO_CATEGORY', 'EXIT_APP', 'GO_TO_CATEGORY', 'GO_TO_CATEGORY']
# List of event categories
event_category = ['LIQUORS', 'PHARMACY', 'TECHNOLOGY', 'ELECTRO_DOMESTIC', 'BABY', 'SUPERMARKET']
# Sequence of final user events
final_user_event = ['PURCHASE', 'EXIT_APP', 'PURCHASE', 'EXIT_APP', 'PURCHASE']

# Coordinates of 5 departments of Guatemala
guate_coords = (14.583433719731175, -90.54504814981065)
pro_coords = (14.747716452097745, -90.11598064639617)
pet_coords = (16.199295697554618, -89.44288359648083)
iza_coords = (15.254832206786691, -89.09696966551803)
sac_coords = (14.531109705961676, -90.75143141168198)

# Call the 'create_masive_users' function to generate a list of user data
user_list = create_masive_users(5000)

# Initialize a counter 'x' and an empty list 'data_purchase'
x = 0
data_purchase = []

# Start a loop to generate and send simulated purchase events
while (x >= 0):
    # Get the current date and time
    date = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")

    # Generate a random event sequence using custom function 'get_second_event'
    event = get_second_event(initial_event, second_event, third_event, operating_systems, cities)

    # Randomly select a user ID from 'user_list'
    user_id = random.choices(user_list)[0]

    # Extract event details from the generated event sequence
    event_user_1 = event[0]
    event_user_2 = event[1]
    event_user_3 = event[2]
    event_user_4 = event[3]
    event_user_os = event[4]
    event_user_city = event[5]
    event_user_order = event[6]
    event_user_status = event[7]
    event_user_payment = event[8]

    # Create a dictionary 'purchase' containing information about the purchase event
    purchase = {
        'USER_ID': user_id,
        'INITIAL_EVENT': event_user_1,
        'EVENT_2': event_user_2,
        'EVENT_3': event_user_3,
        'EVENT_OUT': event_user_4,
        'OS_USER': event_user_os,
        'CITY': event_user_city,
        'LATITUD': get_coords(event_user_city)[0],
        'LONGITUD': get_coords(event_user_city)[1],
        'ORDER_TYPE': event_user_order,
        'STATUS': event_user_status,
        'PAYMENT_METHOD': event_user_payment,
        'CREATED_AT': date
    }

    # Serialize the 'purchase' dictionary to a JSON-encoded string
    record_value = json.dumps(purchase).encode('utf-8')

    # Print the JSON-encoded purchase event
    print(record_value)

    # Uncomment the following lines to send the record to the Kinesis stream
    records = KinesisClient.put_record(StreamName=stream_name, Data=record_value, PartitionKey='USER_ID')
    print("Total data ingested:" + str(x) + ", ReqID:" + records['ResponseMetadata']['RequestId'] +
           ", HTTPStatusCode:" + str(records['ResponseMetadata']['HTTPStatusCode']))

    # Increment the counter 'x'
    x += 1

    # Introduce a random sleep delay before the next iteration
    time.sleep(random.choice([0.5]))

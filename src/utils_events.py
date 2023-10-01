import pandas as pd
import json
import random
import time
import hashlib
import os

# Define lists and variables for simulating events and data

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

# Define a function to generate the second stage of events
def get_second_event(initial_event, second_event, third_event, os, cities):
    # Choose a random event from the second stage
    event_2 = random.choice(second_event)

    if event_2 == 'HOME':

        # If HOME event, choose an event from the third stage
        event_3 = random.choice(third_event)

        if event_3 == 'GO_TO_CATEGORY':

            # If GO_TO_CATEGORY event, choose a last_event and final_event
            last_event = random.choice(event_category)
            final_event = random.choice(final_user_event)

            if final_event == 'PURCHASE':

                # If final event is PURCHASE, set payment, OS, city, status, and order_type
                payment = random.choice(payment_online)
                os_source = random.choice(os)
                city = random.choice(cities)
                status = 'COMPLETED'
                order_type = 'PURCHASE'

            elif final_event == 'EXIT_APP':

                # If final event is EXIT_APP, set payment, OS, city, status, and order_type
                payment = 'Null'
                os_source = random.choice(os)
                city = random.choice(cities)
                status = 'UNCONVERTED'
                order_type = 'USER_VISIT'

        elif event_3 == 'EXIT_APP':

            # If event is EXIT_APP, set payment, OS, city, status, and order_type
            payment = 'Null'
            os_source = random.choice(os)
            city = random.choice(cities)
            status = 'UNCONVERTED'
            order_type = 'USER_VISIT'
            last_event = 'HOME'

    else:

        # If event is not HOME, set payment, OS, city, status, order_type, and last_event
        payment = 'Null'
        os_source = random.choice(os)
        city = random.choice(cities)
        status = 'UNCONVERTED'
        order_type = 'USER_VISIT'
        last_event = 'LAUNCH_APP'
        event_3 = 'Null'

    return initial_event, event_2, event_3, last_event, os_source, city, order_type, status, payment


# Define a function to retrieve coordinates based on city
def get_coords(city):
    if city == 'Guatemala':
        coords = guate_coords
    elif city == 'Progreso':
        coords = pro_coords
    elif city == 'Peten':
        coords = pet_coords
    elif city == 'Izabal':
        coords = iza_coords
    elif city == 'Sacatepequez':
        coords = sac_coords

    return coords


# Define a function to create a list of user data
def create_masive_users(n_users):
    users_bank = []

    for i in range(n_users):
        # Generate a date and hash it to create a user ID
        date = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%%S")
        users_bank.append(str(hashlib.sha256(f"{i} {date}".encode('utf-8')).hexdigest())[:10])

    return users_bank

#!/usr/bin/env python3

import csv
from configparser import ConfigParser
from datetime import datetime
from time import sleep

import requests
from confluent_kafka import Producer

# Weather data URLs
areas = 'areas.csv'
base_url = 'https://ibnux.github.io/BMKG-importer/cuaca/'

# Kafka configuration
config_file = 'getting_started.ini'
config_parser = ConfigParser()

with open(config_file, 'r', encoding='utf-8') as file:
    config_parser.read_file(file)

config = dict(config_parser['default'])

# Kafka producer
topic = 'weather'
producer = Producer(config)

def delivery_callback(err, msg):
    """ Called once for each message produced to indicate delivery result. """

    if err:
        print(f"Message failed delivery: {err}")
    else:
        print(
            f"Produced event to topic {msg.topic()}: key = {msg.key()} value = {msg.value()}"
        )

def publish_weather_data(url):
    """ Publish weather data to Kafka topic. """

    response = requests.get(url, timeout=5)

    data = {
        'status' : '',
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'error_message': '',
        'body': None
    }

    if response.status_code == 200:
        response_json = response.json()

        data['status'] = 'success'
        data['error_message'] = None
        data['body'] = response_json

    else:
        data['status'] = 'error'
        data['error_message'] = f"Failed to fetch weather data from {url}"
        data['body'] = None

    producer.produce(
        topic,
        value=str(data).encode('utf-8'),
        key=url.split('/')[-1].encode('utf-8'),
        callback=delivery_callback
    )

    producer.poll(500)  # Trigger message delivery

def main():
    """ Main function. """

    with open(areas, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        urls = [base_url + row['id'] + '.json' for row in reader]

    while True:
        try:
            for url in urls:
                publish_weather_data(url)

            producer.flush()  # Ensure all messages are delivered
            sleep(2)
        
        except KeyboardInterrupt:
            break


if __name__ == '__main__':
    main()
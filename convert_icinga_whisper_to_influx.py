#!/usr/bin/env python3

import os
import re
import datetime
import logging
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.exceptions import InfluxDBError
from whisper import fetch, create, update, info
import yaml

# Load configuration file
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Create InfluxDB client
client = InfluxDBClient(url=config['influxdb']['url'], token=config['influxdb']['token'])

# Get bucket and organization from configuration file
bucket = config['influxdb']['bucket']
org = config['influxdb']['org']

# Create query client
query_api = client.query_api()

# Function to sanitize variables
def sanitize(var):
    if var == 'metric':
        return re.sub(r'[^a-zA-Z0-9_/]', '_', var)
    else:
        return re.sub(r'[^a-zA-Z0-9_]', '_', var)

# Function to convert Whisper files to Line Protocol
def convert_whisper_to_line_protocol(file_path, hostname, service, metric):
    try:
        # Read information about the Whisper file
        info_data = info(file_path)

        # Determine start and end time for conversion
        start_time = config['start_date']
        end_time = datetime.datetime.now() - datetime.timedelta(minutes=5)

        # Read data from the Whisper file
        data = fetch(file_path, start=start_time, end=end_time)

        # Convert data to Line Protocol
        line_protocol_data = []
        for timestamp, value in data:
            line_protocol_data.append(f"{metric},hostname={hostname},service={service} value={value} {int(timestamp)}")

        # Write data to InfluxDB
        write_api = client.write_api()
        write_api.write(bucket, org, line_protocol_data)

        # Log success
        logging.info(f"File {file_path} successfully converted and written to InfluxDB.")

    except Exception as e:
        # Log error
        logging.error(f"Error converting {file_path}: {str(e)}")

# Function to create log file
def create_logfile():
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file_name = f"log_{timestamp}.log"
    logging.basicConfig(filename=log_file_name, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Main function
def main():
    create_logfile()

    # Query to retrieve hostname, service, metric combinations
    query = f"""
        from(bucket: "{bucket}")
        |> range(start: -2d)
        |> filter(fn: (r) => r._measurement == "measurement")
        |> keep(columns: ["hostname", "service", "metric"])
        |> group(columns: ["hostname", "service", "metric"])
    """

    # Read query result
    result = query_api.query(org=org, query=query)

    # Store result as dictionary
    result_dict = {}
    for table in result:
        for record in table.records:
            hostname = record.get('hostname')
            service = record.get('service')
            metric = record.get('metric')
            result_dict[(hostname, service, metric)] = (sanitize(hostname), sanitize(service), sanitize(metric))

    # Iterate over results and convert Whisper files
    for (hostname, service, metric), (sanitized_hostname, sanitized_service, sanitized_metric) in result_dict.items():
        file_path = f"/mig-perf-data/whisper/icinga2/{sanitized_hostname}/services/{sanitized_service}/perfdata/{sanitized_metric}/value.wsp"
        if os.path.exists(file_path):
            logging.info(f"File {file_path} found. Converting to Line Protocol...")
            convert_whisper_to_line_protocol(file_path, hostname, service, metric)
        else:
            logging.info(f"File {file_path} not found.")

if __name__ == "__main__":
    main()

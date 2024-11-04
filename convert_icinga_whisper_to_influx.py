#!/usr/bin/env python3

import logging
import os
import re
import whisper
import yaml
from datetime import datetime, timedelta
from influxdb import InfluxDBClient
import argparse
import urllib3
import sys

# Suppress only the single InsecureRequestWarning from urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Process InfluxDB and convert wsp to line protocol.")
parser.add_argument("--config", required=True, help="Path to the configuration YAML file")
parser.add_argument("--simulate", action="store_true", help="Run in simulation mode (no data will be written)")
parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
args = parser.parse_args()

# Configure logging
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"script_{timestamp}.log"
logging.basicConfig(filename=log_filename, level=logging.INFO, format="%(asctime)s - %(message)s")

# Function to sanitize names
def sanitize_name(name, allow_slash=False):
    name = re.sub(r'[\\/\s\.]+', '_', name)
    if allow_slash:
        name = name.replace('::', '/')
    else:
        name = name.replace('::', '_')
    return name

# Function to construct wsp file path
def construct_wsp_file_path(base_path, hostname, servicename, checkcommand, metric):
    if servicename == "HOSTCHECK":
        wtype = "host"
    else:
        wtype = "services"
        
    return os.path.join(
        base_path,
        sanitize_name(hostname),
        wtype,
        sanitize_name(servicename),
        checkcommand,
        "perfdata",
        sanitize_name(metric, allow_slash=True),
        "value.wsp"
    )

# Function to convert wsp to line protocol and write to InfluxDB
def convert_and_write_to_influx(wsp_path, hostname, servicename, checkcommand, original_metric_name, end_timestamp, influx_client, target_db, simulate, verbose):
    try:
        data_points = whisper.fetch(wsp_path, START_TIMESTAMP, end_timestamp)
        if data_points is None:
            logging.warning(f"No data points found in '{wsp_path}' within the specified range.")
            return

        timestamps, values = data_points
        points_written = 0

        for timestamp, value in zip(timestamps, values):
            # Skip empty values
            if value is None or value == 0:
                continue

            point = {
                "measurement": checkcommand,
                "tags": {
                    "hostname": hostname,
                    "metric": original_metric_name,
                    "service": servicename
                },
                "time": datetime.utcfromtimestamp(timestamp).isoformat(),
                "fields": {
                    "value": value
                }
            }

            if simulate:
                if verbose:
                    print(point)
                logging.info(f"Simulated write: {point}")
            else:
                influx_client.write_points([point], database=target_db)
                if verbose:
                    print(point)
                logging.info(f"Written: {point}")

            points_written += 1

        logging.info(f"Processed '{wsp_path}': {points_written} data points {'simulated' if simulate else 'written'}.")

    except Exception as e:
        logging.error(f"Failed to read from Whisper file '{wsp_path}': {e}")

# Load configuration
with open(args.config, 'r') as file:
    config = yaml.safe_load(file)
logging.info("Configuration loaded successfully.")

influx_config = config['influxdb']
BASE_PATH = config['base_path']
START_TIMESTAMP = int(datetime.strptime(str(config['start_date']), "%Y-%m-%d").timestamp())
UNTIL_TS_OFFSET = config['until_ts_offset']

# Extract scheme, host, and port
url = influx_config['url']
scheme, rest = url.split('://')
host, port = rest.split(':')

# Connect to InfluxDB
client = InfluxDBClient(
    host=host,
    port=int(port),
    username=influx_config['user'],
    password=influx_config['password'],
    database=influx_config['source_db'],
    ssl=(scheme == 'https'),
    verify_ssl=False  # Disable SSL verification
)
logging.info(f"Connected to InfluxDB at {url}.")

# Query InfluxDB for all measurements
measurements = client.get_list_measurements()
logging.info(f"Found {len(measurements)} measurements.")

for measurement in measurements:
    measurement_name = measurement['name']
    logging.info(f"Processing measurement: {measurement_name}")

    # Query each measurement for metrics
    query = f'''
    SELECT FIRST(*) FROM "{measurement_name}"
    WHERE time >= '{config['start_date']}'
    GROUP BY "hostname", "service", "metric"
    '''
    result = client.query(query)
    metrics_count = len(list(result.get_points()))
    logging.info(f"Found {metrics_count} metrics in measurement '{measurement_name}'.")

    for point in result.items():
        # Extract tags from the group key
        hostname = point[0][1]["hostname"]
        servicename = point[0][1]["service"]
        metric = point[0][1]["metric"]
        data = list(point[1])[0]
        if not servicename:
            servicename = "HOSTCHECK"
        
        # Break if any tag is missing
        if not hostname or not metric:
            logging.error(f"Missing required tags in measurement '{measurement_name}': hostname={hostname}, service={servicename}, metric={metric}")
            sys.exit(1)

        end_timestamp = int(datetime.strptime(data['time'], "%Y-%m-%dT%H:%M:%SZ").timestamp()) - UNTIL_TS_OFFSET

        wsp_file_path = construct_wsp_file_path(BASE_PATH, hostname, servicename, measurement_name, metric)

        if os.path.isfile(wsp_file_path):
            logging.info(f"Processing WSP file: {wsp_file_path}")
            convert_and_write_to_influx(
                wsp_file_path, hostname, servicename, measurement_name, metric, end_timestamp, client, influx_config['target_db'], args.simulate, args.verbose
            )
        else:
            logging.warning(f"No 'value.wsp' file found at path: '{wsp_file_path}' for metric '{metric}', service '{servicename}', checkcommand '{measurement_name}'.")

#!/usr/bin/env python3

import logging
import os
import re
import whisper
import yaml
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import argparse

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
    return os.path.join(
        base_path,
        sanitize_name(hostname),
        "services",
        sanitize_name(servicename),
        checkcommand,
        "perfdata",
        sanitize_name(metric, allow_slash=True),
        "value.wsp"
    )

# Function to convert wsp to line protocol and write to InfluxDB
def convert_and_write_to_influx(wsp_path, hostname, servicename, checkcommand, original_metric_name, end_timestamp, influx_client, target_bucket, org, simulate, verbose):
    try:
        data_points = whisper.fetch(wsp_path, START_TIMESTAMP, end_timestamp)
        if data_points is None:
            logging.warning(f"No data points found in '{wsp_path}' within the specified range.")
            return

        _, values = data_points
        points_written = 0

        for timestamp, value in zip(data_points[0], values):
            if value is None:
                continue

            point = Point(checkcommand) \
                .tag("hostname", hostname) \
                .tag("metric", original_metric_name) \
                .tag("service", servicename) \
                .field("value", value) \
                .time(timestamp)

            if simulate:
                if verbose:
                    print(point.to_line_protocol())
                logging.info(f"Simulated write: {point.to_line_protocol()}")
            else:
                write_api = influx_client.write_api(write_options=SYNCHRONOUS)
                write_api.write(bucket=target_bucket, org=org, record=point)
                if verbose:
                    print(point.to_line_protocol())
                logging.info(f"Written: {point.to_line_protocol()}")

            points_written += 1

        logging.info(f"Processed '{wsp_path}': {points_written} data points {'simulated' if simulate else 'written'}.")

    except Exception as e:
        logging.error(f"Failed to read from Whisper file '{wsp_path}': {e}")

# Load configuration
with open(args.config, 'r') as file:
    config = yaml.safe_load(file)

influx_config = config['influxdb']
BASE_PATH = config['base_path']
START_TIMESTAMP = int(datetime.strptime(config['start_date'], "%Y-%m-%d").timestamp())
UNTIL_TS_OFFSET = int(timedelta(minutes=int(config['until_ts_offset'].strip('m'))).total_seconds())

# Connect to InfluxDB
client = InfluxDBClient(url=influx_config['url'], token=influx_config['token'], org=influx_config['org'])

# Query InfluxDB for metrics
query_api = client.query_api()
query = f'''
from(bucket: "{influx_config['source_bucket']}")
|> range(start: {config['start_date']})
|> filter(fn: (r) => r._measurement == "checkcommand")
|> group(columns: ["hostname", "service", "metric"])
|> sort(columns: ["_time"], desc: false)
|> first()
'''
result = query_api.query(org=influx_config['org'], query=query)

for table in result:
    for record in table.records:
        hostname = record['hostname']
        servicename = record['service']
        checkcommand = record['_measurement']
        metric = record['metric']
        end_timestamp = int(record['_time'].timestamp()) - UNTIL_TS_OFFSET

        wsp_file_path = construct_wsp_file_path(BASE_PATH, hostname, servicename, checkcommand, metric)

        if os.path.isfile(wsp_file_path):
            convert_and_write_to_influx(
                wsp_file_path, hostname, servicename, checkcommand, metric, end_timestamp, client, influx_config['target_bucket'], influx_config['org'], args.simulate, args.verbose
            )
        else:
            logging.warning(f"No 'value.wsp' file found at path: '{wsp_file_path}' for metric '{metric}'.")

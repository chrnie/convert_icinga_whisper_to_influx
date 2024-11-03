#!/usr/bin/env python3
 
import csv
import argparse
import logging
import sys
import os
import re
import gzip
import whisper
from datetime import datetime
 
# Configure logging
logging.basicConfig(filename="script.log", level=logging.INFO, format="%(asctime)s - %(message)s")
 
# Define the base path for wsp files
BASE_PATH = "/mig-perf-data/whisper/icinga2"
START_TIMESTAMP = int(datetime(2022, 1, 1).timestamp())
 
def sanitize_name(name, allow_slash=False):
    """
    Sanitizes names for filesystem usage:
    - Replaces backslashes, forward slashes, dots, and spaces with '_'.
    - Replaces '::' with '/' if allow_slash=True, otherwise replaces '::' with '_'.
    """
    name = re.sub(r'[\\/\s\.]+', '_', name)  # Replace \, /, . and spaces with _
    if allow_slash:
        name = name.replace('::', '/')  # Convert '::' to '/' only when constructing directory paths
    else:
        name = name.replace('::', '_')  # Keep '::' as '_' in filenames
    return name
 
def parse_performance_data(perf_data):
    """
    Parses the performance data string from Icinga and returns a dictionary mapping original and sanitized metric names.
    """
    metrics = {}
    #pattern = r"('.*?'|[^=;\s]+)=(\S+)"
    pattern = r"((?:'[^']*'|\S+))=(\S+)"
    matches = re.findall(pattern, perf_data)
 
    for label, _ in matches:
        original_label = label.strip("'")  # Remove surrounding quotes if any
        sanitized_label = sanitize_name(original_label, allow_slash=True)  # sanitize with slashes for path
        metrics[original_label] = sanitized_label
 
    return metrics
 
def construct_wsp_file_path(hostname, servicename, checkcommand, metric):
    """
    Constructs the path to the 'value.wsp' file for a given metric.
    """
    search_path = os.path.join(
        BASE_PATH,
        sanitize_name(hostname),
        "services",
        sanitize_name(servicename),
        checkcommand,
        "perfdata",
        sanitize_name(metric, allow_slash=True),
        "value.wsp"
    )
    return search_path
 
def convert_to_line_protocol(wsp_path, hostname, servicename, checkcommand, original_metric_name, end_timestamp):
    """
    Reads data from the wsp file and converts it to line protocol format using the original metric name.
    """
    line_protocol_data = []
    try:
        # Fetch data points from Whisper file within the specified range
        data_points = whisper.fetch(wsp_path, START_TIMESTAMP, end_timestamp)
        if data_points is None:
            logging.warning(f"No data points found in '{wsp_path}' within the specified range.")
            return None
 
        _, values = data_points
        for timestamp, value in zip(data_points[0], values):
            if value is None:  # Skip empty data points
                continue
 
            line_protocol = (
                f"{checkcommand},hostname={hostname},metric={original_metric_name},service={servicename} "
                f"value={value} {timestamp}\n"
            )
            line_protocol_data.append(line_protocol)
 
        return ''.join(line_protocol_data)
 
    except Exception as e:
        logging.error(f"Failed to read from Whisper file '{wsp_path}': {e}")
        return None
 
def save_line_protocol(wsp_path, line_protocol_data):
    """
    Saves the line protocol data to a compressed 'value.lp.gz' file in the same directory as the provided wsp file.
    """
    lp_gz_path = os.path.join(os.path.dirname(wsp_path), "value.lp.gz")
 
    if os.path.isfile(lp_gz_path):
        logging.info(f"File '{lp_gz_path}' already exists. Skipping conversion.")
        return lp_gz_path
 
    with gzip.open(lp_gz_path, "wt") as lp_file:
        lp_file.write(line_protocol_data)
    logging.info(f"Converted '{wsp_path}' to line protocol and saved as '{lp_gz_path}'")
    return lp_gz_path
 
# Parse arguments
parser = argparse.ArgumentParser(description="Process CSV file and convert wsp to line protocol.")
parser.add_argument("--csv", required=True, help="Path to the CSV file")
parser.add_argument("--end-timestamp", required=True, type=int, help="End timestamp for data extraction")
args = parser.parse_args()
 
try:
    with open(args.csv, mode="r", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader)
 
        if headers[:4] != ["host.name", "name", "checkcommand_name", "state.performance_data"]:
            logging.error("CSV header is incorrect. Expected: 'host.name', 'name', 'checkcommand_name', 'state.performance_data'")
            sys.exit("CSV header is incorrect. Expected: 'host.name', 'name', 'checkcommand_name', 'state.performance_data'")
 
        for row in reader:
            hostname, servicename, checkcommand, perf_data = row[:4]
            metrics = parse_performance_data(perf_data)
            metric_info = []
            for original_metric_name, sanitized_metric_name in metrics.items():
                wsp_file_path = construct_wsp_file_path(hostname, servicename, checkcommand, original_metric_name)
 
                if os.path.isfile(wsp_file_path):
                    line_protocol_data = convert_to_line_protocol(
                        wsp_file_path, hostname, servicename, checkcommand, original_metric_name, args.end_timestamp
                    )
                    if line_protocol_data:
                        save_line_protocol(wsp_file_path, line_protocol_data)
                        metric_info.append(f"{original_metric_name} (converted)")
                else:
                    logging.warning(f"No 'value.wsp' file found at path: '{wsp_file_path}' for metric '{original_metric_name}' (sanitized: '{sanitized_metric_name}').")
                    metric_info.append(f"{original_metric_name} (missing)")
 
            logging.info(
                f"Processed row for hostname '{hostname}', service '{servicename}', checkcommand '{checkcommand}': "
                f"Metrics found - {', '.join(metric_info)}"
            )
 
except FileNotFoundError:
    logging.error(f"CSV file {args.csv} not found.")
    sys.exit(f"CSV file {args.csv} not found.")
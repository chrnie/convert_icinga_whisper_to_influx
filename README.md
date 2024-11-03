# InfluxDB WSP to Line Protocol Converter

This Python script reads Whisper data files (WSP) and converts the data into InfluxDB line protocol format, optionally writing it to an InfluxDB database. It allows you to transfer historical data stored in WSP files to InfluxDB, maintaining data granularity and flexibility.

## Prerequisites

- Python 3
- Required Python libraries (install using `pip install -r requirements.txt`):
  - `influxdb_client`
  - `whisper`
  - `PyYAML`

## Usage

```bash
python convert_icinga_whisper_to_influx.py --config <path/to/config.yml> [--simulate] [--verbose]
```

### Arguments

- `--config`: **(Required)** Path to the YAML configuration file containing InfluxDB and file path settings.
- `--simulate`: Runs the script in simulation mode, logging the data points that would be written to InfluxDB without actually writing them.
- `--verbose`: Enables verbose output, printing each data point's line protocol to the console.

## Configuration File (`config.yml`)

The configuration file should include details about your InfluxDB setup, data paths, and timestamps.

### Example Configuration (`config.yml`)

```yaml
---
influxdb:
  url: http://localhost:8086
  token: my_token
  source_bucket: my_sbucket
  target_bucket: my_tbucket
  org: my_org

start_date: 2022-01-01
until_ts_offset: 5m
base_path: /mig-perf-data/whisper/icinga2
```

### Configuration Options

- `influxdb`: Contains InfluxDB connection settings.
  - `url`: InfluxDB server URL.
  - `token`: Authentication token for accessing the database.
  - `source_bucket`: InfluxDB bucket to read data from.
  - `target_bucket`: InfluxDB bucket to write data to.
  - `org`: InfluxDB organization name.
- `start_date`: Start date for querying data (in `YYYY-MM-DD` format).
- `until_ts_offset`: Time offset (in minutes) to adjust the end timestamp.
- `base_path`: Base directory path where WSP files are stored.

## Logging

The script creates a log file named `convert_icinga_whisper_to_influx_<timestamp>.log`, where each run is timestamped for traceability. Logs include processed file paths, warnings, and errors.

## Script Workflow

1. **Parse Command-line Arguments**: Reads the configuration file, simulation, and verbosity flags.
2. **Load Configuration**: Loads InfluxDB and file path settings from the specified YAML configuration file.
3. **Sanitize and Construct File Paths**: Constructs WSP file paths based on the base path, hostname, service name, check command, and metric.
4. **Convert WSP to Line Protocol**: Reads Whisper (WSP) files and converts data points into line protocol format, logging them or writing them to InfluxDB depending on the simulation flag.
5. **Write to InfluxDB**: If not in simulation mode, writes data points to the specified InfluxDB bucket.

## Example

```bash
python convert_icinga_whisper_to_influx.py --config config.yml --simulate --verbose
```

This command will simulate the data conversion, printing each line protocol entry without writing to InfluxDB.

## Error Handling

- The script logs errors encountered when reading WSP files or connecting to InfluxDB.
- Missing WSP files or data points are logged as warnings.

## License

This project is open-source and free to use under the MIT license.

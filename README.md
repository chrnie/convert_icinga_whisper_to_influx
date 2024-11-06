# Script for Converting Whisper Data to InfluxDB Line Protocol

This script processes data from InfluxDB and converts Whisper files (`.wsp`) into the InfluxDB Line Protocol format. It provides options to simulate the conversion and log detailed debug information.
It's specialised on converting icinga2 performance data from graphite to influxDB.

## Prerequisites

- Python 3.x
- Dependencies: `whisper`, `PyYAML`, `influxdb`, `tqdm`

## Installation

1. Clone the repository or download the script.

### pip
2. Install the required Python packages:
   ```bash
   pip install whisper PyYAML influxdb tqdm
   ```

### ubuntu
2. Install the required Python packages:
   ```bash
   apt install python3-tqdm python3-influxdb python3-whisper python3-yaml
   ```

## Usage

Run the script with the required arguments:

```bash
./script.py --config <path-to-configuration-file> [--simulate] [--debug]
```

### Arguments

- `--config`: Path to the YAML configuration file (required).
- `--simulate`: Runs the script in simulation mode without writing data.
- `--debug`: Enables debug output.

## Configuration File

The configuration file should be in YAML format and contain the following information:

```yaml
influxdb:
  url: "http://localhost:8086"
  user: "username"
  password: "password"
  source_db: "source_database"
  target_db: "target_database"
base_path: "/path/to/whisper/files"
start_date: "2023-01-01"
until_ts_offset: 0
```

## How It Works

1. The script connects to the specified InfluxDB instance.
2. It retrieves all measurements and processes each one individually.
3. For each measurement, it queries the associated metrics and processes the corresponding Whisper files.
4. The data is converted into the Line Protocol format and written to the target InfluxDB database.

## Progress Display

The script uses `tqdm` to display the progress of processing each measurement and metric.

## Logging

The script creates a log file with detailed information about the processing steps and any errors encountered.

## Script Workflow

1. **Parse Command-line Arguments**: Reads the configuration file, simulation, and verbosity flags.
2. **Load Configuration**: Loads InfluxDB and file path settings from the specified YAML configuration file.
3. **Explore Metrics**: Query the name and oldest datapoint of every metric per hostname and service
3. **Sanitize and Construct File Paths**: Constructs WSP file paths based on the base path, hostname, service name, check command, and metric.
4. **Convert WSP to Line Protocol**: Reads Whisper (WSP) files and converts data points into line protocol format, logging them or writing them to InfluxDB depending on the simulation flag.
5. **Write to InfluxDB**: If not in simulation mode, writes data points to the specified InfluxDB database.


## License

This project is licensed under the MIT License.

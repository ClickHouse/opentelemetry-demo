#!/usr/bin/env python3
import json
import tarfile
import os
import sys
import time
from datetime import datetime


def process_logs_json(input_filepath, output_filepath=None):
    """
    Process logs.json to find minimum timeUnixNano and observedTimeUnixNano,
    then create a new file with times updated relative to now

    Args:
        input_filepath (str): Path to the logs.json file
        output_filepath (str, optional): Path for the output file
                                         If not provided, will use "updated_logs.json"

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Set default output file if not provided
        if not output_filepath:
            output_dir = os.path.dirname(input_filepath)
            output_filepath = os.path.join(output_dir, "updated_logs.json")

        print(f"Processing {input_filepath}...")

        # Read and parse the JSON file
        # Each line is a separate JSON object
        min_time_unix_nano = float('inf')
        min_observed_time_unix_nano = float('inf')
        log_entries = []

        with open(input_filepath, 'r') as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue

                log_entry = json.loads(line)
                log_entries.append(log_entry)

                # Find minimum values by examining all logRecords in all resourceLogs
                for resource_log in log_entry.get('resourceLogs', []):
                    for scope_log in resource_log.get('scopeLogs', []):
                        for log_record in scope_log.get('logRecords', []):
                            if "timeUnixNano" in log_record:
                                time_unix_nano = int(log_record["timeUnixNano"])
                            if "observedTimeUnixNano" in log_record:
                                observed_time_unix_nano = int(log_record["observedTimeUnixNano"])
                            if time_unix_nano < min_time_unix_nano:
                                min_time_unix_nano = time_unix_nano

                            if observed_time_unix_nano < min_observed_time_unix_nano:
                                min_observed_time_unix_nano = observed_time_unix_nano

        # If no valid entries found
        if min_time_unix_nano == float('inf') or min_observed_time_unix_nano == float('inf'):
            print("Error: No valid timeUnixNano or observedTimeUnixNano found in logs")
            return False

        # Get current time in nanoseconds
        current_time_nano = int(time.time_ns())

        # Calculate time offsets
        time_offset = current_time_nano - min_time_unix_nano
        observed_time_offset = current_time_nano - min_observed_time_unix_nano

        # Format human-readable times for display
        min_time_str = datetime.fromtimestamp(min_time_unix_nano / 1e9).strftime('%Y-%m-%d %H:%M:%S.%f')
        min_observed_time_str = datetime.fromtimestamp(min_observed_time_unix_nano / 1e9).strftime(
            '%Y-%m-%d %H:%M:%S.%f')
        current_time_str = datetime.fromtimestamp(current_time_nano / 1e9).strftime('%Y-%m-%d %H:%M:%S.%f')

        print(f"Minimum timeUnixNano: {min_time_unix_nano} ({min_time_str})")
        print(f"Minimum observedTimeUnixNano: {min_observed_time_unix_nano} ({min_observed_time_str})")
        print(f"Current time: {current_time_nano} ({current_time_str})")

        # Update times in the log entries
        updated_entries = []
        for log_entry in log_entries:
            updated_entry = log_entry.copy()

            for resource_log in updated_entry.get('resourceLogs', []):
                for scope_log in resource_log.get('scopeLogs', []):
                    for log_record in scope_log.get('logRecords', []):
                        if 'timeUnixNano' in log_record:
                            original_time = int(log_record['timeUnixNano'])
                            log_record['timeUnixNano'] = str(original_time + time_offset)

                        if 'observedTimeUnixNano' in log_record:
                            original_observed_time = int(log_record['observedTimeUnixNano'])
                            log_record['observedTimeUnixNano'] = str(original_observed_time + observed_time_offset)

            updated_entries.append(updated_entry)

        # Write updated entries to output file
        with open(output_filepath, 'w') as out_file:
            for entry in updated_entries:
                out_file.write(json.dumps(entry) + '\n')

        print(f"Successfully updated log times and saved to {output_filepath}")
        return True

    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {str(e)}")
        return False
    except Exception as e:
        print(f"Error processing logs: {str(e)}")
        return False



def decompress_tar_gz(filepath):
    """
    Decompress a .tar.gz file to a 'sample' subdirectory of the current directory.
    """
    try:
        if not os.path.exists(filepath):
            print(f"Error: File {filepath} does not exist")
            return False

        extract_dir = os.path.join(os.path.dirname(filepath), "sample")
        os.makedirs(extract_dir, exist_ok=True)

        print(f"Extracting {filepath} to {extract_dir}...")
        with tarfile.open(filepath, "r:gz") as tar:
            tar.extractall(path=extract_dir)

        expected_files = ["logs.json", "traces.json", "metrics.json"]
        found_files = os.listdir(extract_dir)
        missing_files = [f for f in expected_files if f not in found_files]

        if missing_files:
            print(f"Warning: The following expected files were not found: {', '.join(missing_files)}")
        else:
            print(f"Successfully extracted all expected files to {extract_dir}/")

        return True

    except tarfile.ReadError:
        print(f"Error: {filepath} is not a valid tar.gz file")
        return False
    except Exception as e:
        print(f"Error during extraction: {str(e)}")
        return False


if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = "sample.tar.gz"

    success = decompress_tar_gz(file_path)
    if success:
        extracted_dir = os.path.join(os.path.dirname(file_path), "sample")
        logs_path = os.path.join(extracted_dir, "logs.json")
        process_logs_json(logs_path)  # output file will also go to same directory
        sys.exit(0)
    else:
        sys.exit(1)

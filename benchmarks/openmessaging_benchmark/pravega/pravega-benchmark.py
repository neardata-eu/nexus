'''
This script automates the running of OBM against a streaming framework, with it optionally connected to Nexus

Arguments are:
    - -b -> S3 bucket name
    - -d -> Driver file
    - -w -> Workload directory
    - -r -> optional results directory

For each workload, the execution writes two files to experiments/<result_dir>/
    - The JSON output of the current workload
    - Appending bucket information before and after the
      execution to experiments/<result_dir>/recorded_ratios.json  
'''

import json
import boto3
import argparse
from datetime import datetime
import subprocess
import sys
import time
import re
import os
import signal

ACCESS_ID  = 'minioadmin'
ACCESS_KEY = 'minioadmin'
ENDPOINT   = 'http://minio-svc:9000'

def run_workload(driver, workload_directory):
    COMMAND = ["bin/benchmark", "--drivers", driver, workload_directory]

    print(f"\n--------------------------------------------- Executing workload: {workload_directory} ---------------------------------------------")
    results_file = ""
    try:
        process = subprocess.Popen(COMMAND, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1, universal_newlines=True, preexec_fn=os.setsid)
        for line in iter(process.stdout.readline, ''):
            sys.stdout.write(line)
            if re.search(r" Writing test result", line):
                captured_line = line.strip();
                results_file = captured_line.split(" ")[-1]
                print(f"Captured log: {captured_line}.\nWaiting a bit for OMB and Pravega to finish their processes...")
                time.sleep(15)
                process.terminate()
                break
        time.sleep(5)
        process.stdout.close()
        os.killpg(os.getpgid(process.pid), signal.SIGKILL) 

    except:
        print("Error running workload.")
        process.stdout.close()
        os.killpg(os.getpgid(process.pid), signal.SIGKILL) 
        exit()
    finally:
        print("Subprocess finished. Continuing with the script...")
        return results_file


def get_workloads(directory):
    directory = args.workloads;
    if not os.path.exists(directory):
        print(f"Directory '{directory}' does not exist")
        exit()

    print(f"Reading workloads in {args.workloads}...")
    return [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]

def get_workload_transfer_size(workload_directory):
    producer_rate_pattern = r"producerRate: (\d+)"
    test_duration_pattern = r"testDurationMinutes: (\d+)"
    message_size_pattern = r"messageSize: (\d+)"

    try:
        with open(workload_directory, 'r') as f:
            content = f.read()

            # Extract producer rate, test duration, and message size
            producer_rate_match = re.search(producer_rate_pattern, content)
            test_duration_match = re.search(test_duration_pattern, content)
            message_size_match = re.search(message_size_pattern, content)

            if producer_rate_match and test_duration_match and message_size_match:
                producer_rate = int(producer_rate_match.group(1))
                test_duration_minutes = int(test_duration_match.group(1))
                message_size = int(message_size_match.group(1))

                test_duration_seconds = (test_duration_minutes + 1) * 60
                total_transferred_data_size = (producer_rate * test_duration_seconds) * message_size

                return total_transferred_data_size, producer_rate, message_size
            else:
                print("Failed to extract relevant information from the file.")
                return None
    except FileNotFoundError:
        print(f"File '{workload_directory}' not found.")
        return None

def get_resource():
	return boto3.resource('s3',
        endpoint_url          = ENDPOINT,
        aws_access_key_id     = ACCESS_ID,
        aws_secret_access_key = ACCESS_KEY,
        aws_session_token     = None,
        verify                = False)

def report(bucket):
    count = 0
    total = 0
    resource = get_resource()
    folder_prefix = "scopeTest"

    if not folder_prefix.endswith('/'):
        folder_prefix += '/'

    iterator = resource.Bucket(bucket).objects.all()
    for obj in iterator:
        if obj.key.startswith(folder_prefix):
            count = count + 1
            total = total + obj.size

    return count, total

def record(output, results_file, optional_dir):
    aggregatedPublishLatencyAvg = 0
    aggregatedPublishLatency95pct = 0
    aggregatedPublishLatency99pct = 0
    aggregatedPublishLatencyMax = 0

    try:
        with open(results_file) as f:
            loaded_json = json.load(f)
            aggregatedPublishLatencyAvg = loaded_json["aggregatedPublishLatencyAvg"]
            aggregatedPublishLatency95pct = loaded_json["aggregatedPublishLatency95pct"]
            aggregatedPublishLatency99pct = loaded_json["aggregatedPublishLatency99pct"]
            aggregatedPublishLatencyMax = loaded_json["aggregatedPublishLatencyMax"]
    except Exception as e:
        print(f'Result file reading error: {e}')

    try:
        if not os.path.exists(f"experiments/results/pravega/{optional_dir}/"):
            os.makedirs(f"experiments/results/pravega/{optional_dir}/")
            print(f"Created results directory: {f'experiments/results/pravega/{optional_dir}/'}")
        results_directory = f"experiments/results/pravega/{optional_dir}/{results_file}"
        os.rename(results_file, results_directory)
        print(f"File {results_file} moved successfully")
    except FileNotFoundError as e:
        print(f"File {results_file} not found to move to results directory: {e}")
        return

    try:
        recorded_ratios = f"experiments/results/pravega/{optional_dir}/recorded_ratios.json"
        os.makedirs(os.path.dirname(recorded_ratios), exist_ok=True)

        if os.path.exists(recorded_ratios) and os.path.getsize(recorded_ratios) > 0:
            with open(recorded_ratios, 'r') as f:
                data = json.load(f)
        else:
            data = {}

        data[results_file.split(".")[0]] = {
            "producerRate" : output["producer_rate"],
            "messageSize" : output['message_size'],
            "originalSize": output['workload_total_size'],
            "bucketSize": output['tiered_storage_size'],
            "compressionRatio": output['workload_total_size']/output['tiered_storage_size'],
            "aggregatedPublishLatencyAvg" : aggregatedPublishLatencyAvg,
            "aggregatedPublishLatency95pct" : aggregatedPublishLatency95pct,
            "aggregatedPublishLatency99pct" : aggregatedPublishLatency99pct,
            "aggregatedPublishLatencyMax" : aggregatedPublishLatencyMax
        }

        with open(recorded_ratios, 'w') as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        print(f"Error recording compression results: {e}")
        return

    print(f"Workload {results_file.split('.')[0]} results recorded successfully: {data[results_file.split('.')[0]]}")
    time.sleep(5)

def delete_bucket_files(bucket):
    try:
        resource = get_resource()
        resource.Bucket(bucket).objects.filter(Prefix="scopeTest/").delete()
        print(f"{bucket} cleared of tiered data")
    except Exception as e:
        print(f"Error clearing the bucket: {e}")
        exit()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--workloads", "-w", action="store", default=None)
    parser.add_argument("--driver", "-d", action="store", default=None)
    parser.add_argument("--bucket", "-b", action="store", default=None)
    parser.add_argument("--results", "-r", action="store", default=None)

    args = parser.parse_args()
    if args.bucket == None or args.workloads == None or args.driver == None:
        print("Please specify a bucket (-b), a workload directory (-w), and an execution driver (-d).")
        exit()
    
    files = get_workloads(args.workloads)

    for file in files:
        workload_directory = os.path.join(args.workloads, file)
        total_workload_transfer_size, producer_rate, message_size = get_workload_transfer_size(workload_directory)
        delete_bucket_files(args.bucket)
        results_json = run_workload(args.driver, workload_directory)

        if(results_json == None or results_json == ''):
            print(f"{file} workload execution failed")
            exit()

        output = {"now": str(datetime.now()), 'bucket': args.bucket}
        count, total = report(args.bucket)
        output['count'] = count
        output['workload_total_size'] = total_workload_transfer_size
        output['tiered_storage_size']  = total
        output['producer_rate'] = producer_rate
        output['message_size'] = message_size

        record(output, results_json, args.results)
        delete_bucket_files(args.bucket)


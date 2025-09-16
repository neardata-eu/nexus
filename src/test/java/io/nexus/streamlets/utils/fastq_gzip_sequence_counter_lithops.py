import lithops
import boto3
import gzip
import time

BUCKET = 'nexus-data-3'
KEY = 'SRR11478824_R1.fastq.gz'


def count_fastq_records_entire_file(_):
    start_time = time.time()
    s3 = boto3.client('s3')

    # Stream the compressed file straight from S3
    response = s3.get_object(Bucket=BUCKET, Key=KEY)
    stream = response['Body']

    # Wrap the streaming body in a GzipFile for on-the-fly decompression
    gz = gzip.GzipFile(fileobj=stream)

    count = 0
    line_buffer = []

    # Read line by line, count one record every 4 lines
    for raw in gz:
        line_buffer.append(raw)
        if len(line_buffer) == 4:
            count += 1
            line_buffer.clear()

    latency_ms = (time.time() - start_time) * 1000
    return {'count': count, 'latency_ms': latency_ms}


def main():
    start = time.time()
    fexec = lithops.FunctionExecutor()
    future = fexec.call_async(count_fastq_records_entire_file, None)
    result = future.result()
    total = result['count']
    latency = result['latency_ms']

    job_duration_ms = (time.time() - start) * 1000

    print("FASTQ GZIP SEQUENCE COUNTER — Single Lambda Streaming")
    print(f"\n✅ Total FASTQ Sequences: {total}")
    print(f"⏱️  Function latency: {latency:.2f} ms")
    print(f"🧭 Job duration:   {job_duration_ms:.2f} ms")


if __name__ == '__main__':
    main()

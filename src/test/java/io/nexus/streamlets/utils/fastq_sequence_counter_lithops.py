import lithops
import boto3
import time
import io

BUCKET = 'nexus-data-3'
KEY = 'SRR11478824_R1.fastq'
CHUNK_SIZE_MB = 75


def count_fastq_records(payload):
    start_time = time.time()
    start, end = payload['range_tuple']

    s3 = boto3.client('s3')
    byte_range = f'bytes={start}-{end}'
    response = s3.get_object(Bucket=BUCKET, Key=KEY, Range=byte_range)

    # Wrap StreamingBody for line-by-line text reading
    stream = io.TextIOWrapper(response['Body'], encoding='utf-8', errors='ignore')

    count = 0
    for i, line in enumerate(stream):
        if i % 4 == 3:
            count += 1

    latency_ms = (time.time() - start_time) * 1000
    return {'count': count, 'latency_ms': latency_ms}


def get_chunk_payloads(file_size, chunk_size):
    ranges = []
    start = 0
    while start < file_size:
        end = min(start + chunk_size - 1, file_size - 1)
        ranges.append({'payload': {'range_tuple': (start, end)}})
        start = end + 1
    return ranges


def main():
    start = time.time()
    s3 = boto3.client('s3')
    metadata = s3.head_object(Bucket=BUCKET, Key=KEY)
    file_size = metadata['ContentLength']
    chunk_size = CHUNK_SIZE_MB * 1024 * 1024

    range_payloads = get_chunk_payloads(file_size, chunk_size)

    fexec = lithops.FunctionExecutor()
    futures = fexec.map(count_fastq_records, range_payloads)
    results = fexec.get_result(futures)
    end = time.time()

    total = sum(r['count'] for r in results)
    latencies = [r['latency_ms'] for r in results]

    print(f"FASTQ SEQUENCE COUNTER - {CHUNK_SIZE_MB}MB chunks")
    print(f"\n✅ Total FASTQ Sequences: {total}")
    print(f"⏱️  Min latency: {min(latencies):.2f} ms | Max: {max(latencies):.2f} ms | Avg: {sum(latencies)/len(latencies):.2f} ms")
    job_duration_ms = (end - start) * 1000
    print(f"\n🧭 Job duration: {job_duration_ms:.2f} ms")


if __name__ == '__main__':
    main()

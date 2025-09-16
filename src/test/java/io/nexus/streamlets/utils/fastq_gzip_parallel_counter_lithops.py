import lithops
import zlib
import math
import time

BUCKET = 'nexus-data-3'
KEY = 'scope-fastqgzip-8/test1/SRR11478824_R1.fastq'
NUM_FUNCTIONS = 19


def get_offsets_from_metadata(bucket, key, storage):
    obj = storage.head_object(bucket, key)
    offset_str = obj['x-amz-meta-fastqgzip-index']
    return list(map(int, offset_str.split(',')))


def chunk_offsets(offsets, num_chunks):
    ranges = []
    num_offsets = len(offsets)
    chunk_size = math.ceil(num_offsets / num_chunks)
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, num_offsets)
        if start_idx >= end_idx:
            break
        start = offsets[start_idx]
        end = offsets[end_idx] if end_idx < num_offsets else None
        ranges.append((start, end))
    return ranges


def stream_gzip_lines(streaming_body):
    decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)
    buffer = b""
    while True:
        chunk = streaming_body.read(8192)
        if not chunk:
            break
        data = decompressor.decompress(chunk)
        buffer += data
        while b'\n' in buffer:
            line, buffer = buffer.split(b'\n', 1)
            yield line.decode('utf-8', errors='ignore')
    remainder = decompressor.flush()
    buffer += remainder
    if buffer:
        for line in buffer.split(b'\n'):
            yield line.decode('utf-8', errors='ignore')


def process_gzip_range(streaming_body):
    lines = []
    for line in stream_gzip_lines(streaming_body):
        lines.append(line)
    full_record_lines = len(lines) // 4 * 4
    return full_record_lines // 4


def fastq_worker(params):
    start_time = time.time()
    bucket = params['bucket']
    key = params['key']
    start = params['start']
    end = params['end']
    storage = lithops.Storage()
    byte_range = f"bytes={start}-{end - 1}" if end is not None else f"bytes={start}-"
    print("Processing byte range", byte_range)
    obj_stream = storage.get_object(bucket, key, extra_get_args={'Range': byte_range}, stream=True)
    print("Getting object range")
    count = process_gzip_range(obj_stream)
    print("Count: ", count)
    latency_ms = (time.time() - start_time) * 1000
    print("Latency: ", latency_ms)
    return {'count': count, 'latency_ms': latency_ms}


def main(bucket, key, num_functions):
    start = time.time()
    storage = lithops.Storage()
    offsets = get_offsets_from_metadata(bucket, key, storage)
    print("OFFSETS", offsets)
    ranges = chunk_offsets(offsets, num_functions)
    print("RANGES", ranges)
    job_params = [{'params': {'bucket': bucket, 'key': key, 'start': s, 'end': e}} for s, e in ranges]

    fexec = lithops.FunctionExecutor()
    results = fexec.map(fastq_worker, job_params)
    results = fexec.get_result(results)

    total = sum(r['count'] for r in results)
    latencies = [r['latency_ms'] for r in results]
    duration_ms = (time.time() - start) * 1000

    print(f"\n📦 COMPRESSED FASTQ SEQUENCE COUNTER - {num_functions} workers")
    print(f"✅ Total FASTQ Sequences: {total}")
    print(f"⏱️  Min latency: {min(latencies):.2f} ms | Max: {max(latencies):.2f} ms | Avg: {sum(latencies)/len(latencies):.2f} ms")
    print(f"🧭 Job duration: {duration_ms:.2f} ms")


if __name__ == '__main__':
    main(BUCKET, KEY, NUM_FUNCTIONS)

import lithops
import gzip
import io
import math
import json

def get_offsets_from_metadata(bucket, key, storage):
    obj = storage.head_object(bucket, key)
    metadata_key = 'x-amz-meta-fastqgzip-index'
    if metadata_key not in obj:
        raise ValueError(f'Metadata {metadata_key} not found in object')
    offset_str = obj[metadata_key]
    return list(map(int, offset_str.split(',')))

def chunk_offsets(offsets, num_chunks):
    """Split list of offsets into N ranges."""
    ranges = []
    num_offsets = len(offsets)
    chunk_size = math.ceil(num_offsets / num_chunks)
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, num_offsets)
        if start_idx >= end_idx:
            break
        chunk = offsets[start_idx:end_idx]
        start = chunk[0]
        end = chunk[-1] if end_idx < len(offsets) else None  # Last chunk reads to EOF
        ranges.append((start, end))
    return ranges

def process_gzip_range(data):
    with gzip.GzipFile(fileobj=io.BytesIO(data)) as gz:
        raw = gz.read()
    count = 0
    i = 0
    n = len(raw)
    while i + 8 < n:
        i += 8  # Skip the 8-byte prefix
        if i >= n or raw[i] != ord('{'):
            print(f"INVALID OR INCOMPLETE JSON {n}")
            break  # Invalid or incomplete JSON block
        brace_depth = 0
        started = False
        start_pos = i

        while i < n:
            b = raw[i]
            if b == ord('{'):
                brace_depth += 1
                started = True
            elif b == ord('}'):
                brace_depth -= 1
                if brace_depth == 0 and started:
                    i += 1  # Include the closing brace
                    break
            i += 1

        if brace_depth == 0 and started:
            try:
                json.loads(raw[start_pos:i].decode('utf-8'))
                count += 1
            except json.JSONDecodeError:
                pass
        else:
            break  # Malformed or truncated record

    return count

def fastq_worker(params):
    import lithops
    bucket = params['bucket']
    key = params['key']
    start = params['start']
    end = params['end']
    storage = lithops.Storage()

    # Construct Range header
    if end is not None:
        byte_range = f"bytes={start}-{end - 1}"
    else:
        byte_range = f"bytes={start}-"

    print(f"Fetching range: {byte_range}")
    obj_bytes = storage.get_object(bucket, key, extra_get_args={'Range': byte_range})
    assert obj_bytes[:2] == b'\x1f\x8b', f"Invalid GZIP header: {obj_bytes[:2]}"
    json_records = process_gzip_range(obj_bytes)
    print(f"Counted DNA Json records: {json_records}")
    return json_records


def main(bucket, key, num_functions):
    storage = lithops.Storage()
    offsets = get_offsets_from_metadata(bucket, key, storage)
    ranges = chunk_offsets(offsets, num_functions)
    print(f"Gzip DNA offsets to read: {ranges}")
    job_params = [{'params': {'bucket': bucket, 'key': key, 'start': s, 'end': e}} for s, e in ranges]

    fexec = lithops.FunctionExecutor()
    futures = fexec.map(fastq_worker, job_params)
    results = fexec.get_result()

    total_lines = sum(results)
    print(f"Total lines counted: {total_lines}")


if __name__ == '__main__':
    # Example usage
    S3_BUCKET = 'pravega-lts'
    S3_KEY = 'scope-fastq/stream4/0.#epoch.0.E-4-O-4194304.4a2880b9-e4c4-4e31-a744-642f0fbed882'
    NUM_FUNCTIONS = 8
    main(S3_BUCKET, S3_KEY, NUM_FUNCTIONS)


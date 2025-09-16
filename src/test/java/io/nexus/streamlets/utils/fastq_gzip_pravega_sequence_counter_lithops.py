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
        end = chunk[-1] if end_idx < len(offsets) else None
        ranges.append((start, end))
    return ranges

def count_fastq_records_from_aligned(lines):
    """
    Counts FASTQ records from a list of lines,
    assuming the first line is a valid '@' line.
    """
    count = 0
    i = 0
    n = len(lines)

    while i + 3 < n:
        if not lines[i].startswith('@'):
            i += 1
            continue
        # Basic structure check
        if lines[i+2].startswith('+'):
            count += 1
            i += 4
        else:
            i += 1  # Possibly malformed, try next '@'

    return count

def process_gzip_range(data):
    with gzip.GzipFile(fileobj=io.BytesIO(data)) as gz:
        raw = gz.read()
    text = raw.decode('utf-8', errors='ignore')
    lines = text.strip().splitlines()

    # Skip until we find the first line starting with '@'
    for idx, line in enumerate(lines):
        if line.startswith('@'):
            aligned_lines = lines[idx:]
            break
    else:
        return 0  # No valid FASTQ record found

    return count_fastq_records_from_aligned(aligned_lines)

def fastq_worker(params):
    import lithops
    bucket = params['bucket']
    key = params['key']
    start = params['start']
    end = params['end']
    storage = lithops.Storage()

    byte_range = f"bytes={start}-{end - 1}" if end is not None else f"bytes={start}-"
    print(f"Fetching range: {byte_range}")
    obj_bytes = storage.get_object(bucket, key, extra_get_args={'Range': byte_range})
    assert obj_bytes[:2] == b'\x1f\x8b', f"Invalid GZIP header: {obj_bytes[:2]}"
    num_records = process_gzip_range(obj_bytes)
    print(f"FASTQ records found: {num_records}")
    return num_records

def main(bucket, key, num_functions):
    storage = lithops.Storage()
    offsets = get_offsets_from_metadata(bucket, key, storage)
    ranges = chunk_offsets(offsets, num_functions)
    print(f"Gzip FASTQ offsets to read: {ranges}")
    job_params = [{'params': {'bucket': bucket, 'key': key, 'start': s, 'end': e}} for s, e in ranges]

    fexec = lithops.FunctionExecutor()
    futures = fexec.map(fastq_worker, job_params)
    results = fexec.get_result()

    total_records = sum(results)
    print(f"Total FASTQ records counted: {total_records}")


if __name__ == '__main__':
    # Example usage
    S3_BUCKET = 'pravega-lts'
    S3_KEY = 'examples/openmessagingbenchmark0000000xcKPVc/0.#epoch.0.E-1-O-4194304.98cc20da-ef2f-491c-b175-d9d246e85e35'
    NUM_FUNCTIONS = 8
    main(S3_BUCKET, S3_KEY, NUM_FUNCTIONS)


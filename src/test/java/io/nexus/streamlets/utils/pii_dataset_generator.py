import random
import os

# Sample PII templates
PII_TEMPLATES = [
    "John Doe, john.doe@example.com, +1-555-123-4567",
    "Alice Smith, alice.smith@domain.net, (212) 555-9876",
    "Bob Johnson, 4111 1111 1111 1111, bob.johnson@mail.com",
    "Maria Garcia, maria.garcia@company.org, +49 170 1234567"
]

def inject_pii(text, interval_bytes, total_bytes):
    output = []
    bytes_written = 0
    pii_index = 0

    while bytes_written < total_bytes:
        chunk = text[:min(len(text), interval_bytes)]
        output.append(chunk)
        bytes_written += len(chunk)

        if bytes_written < total_bytes:
            pii = PII_TEMPLATES[pii_index % len(PII_TEMPLATES)]
            output.append("\n" + pii + "\n")
            bytes_written += len(pii) + 2  # +2 for newlines
            pii_index += 1

    return ''.join(output)[:total_bytes]  # trim to total size exactly


def generate_dataset(base_file_path, output_file_path, total_size_bytes, pii_interval_bytes):
    with open(base_file_path, 'r', encoding='utf-8') as f:
        base_text = f.read()

    if not base_text.strip():
        raise ValueError("Base file is empty or unreadable.")

    # Repeat the base text if needed
    repeats = (total_size_bytes // len(base_text)) + 1
    full_text = (base_text * repeats)

    # Inject PII every `pii_interval_bytes`
    result = inject_pii(full_text, pii_interval_bytes, total_size_bytes)

    with open(output_file_path, 'w', encoding='utf-8') as out:
        out.write(result)

    print(f"✅ Dataset generated: {output_file_path} ({total_size_bytes} bytes, PII every {pii_interval_bytes} bytes)")


# Example usage:
if __name__ == "__main__":
    generate_dataset(
        base_file_path='data/pride_and_prejudice.txt',  # smaller text source
        output_file_path='output/pii_test_data.txt',
        total_size_bytes=10 * 1024 * 1024,  # 10MB
        pii_interval_bytes=1 * 1024 * 1024  # inject every 1MB
    )

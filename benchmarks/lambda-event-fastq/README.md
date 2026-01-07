# FastqGzipIndexingLambda

This project provides a Java AWS Lambda function for block-wise GZIP compression and indexing of FASTQ files, with S3 integration and timing/metadata reporting.

## Project Structure

- `src/main/java/io/nexus/streamlets/functions/FastqGzipIndexingLambda.java`: Main Lambda handler and logic.
- `build.gradle`: Gradle build file with all dependencies.
- `putObject.sh`: Bash script to upload files to S3 and record timing.

## Prerequisites

- Java 21 (OpenJDK recommended)
- Gradle (or use the root project's `./gradlew` wrapper)
- AWS CLI (for `putObject.sh`)
- AWS credentials configured (for both CLI and Lambda execution)

## Setup

1. **Clone the repository** (if not already):
   ```sh
   git clone <repo-url>
   cd nexus
   ```

2. **Build the project**
   - From the root project:
     ```sh
     ./gradlew :lambda-event-fastq:shadowJar
     ```
   - Or from this folder (if Gradle is installed):
     ```sh
     gradle shadowJar
     ```
   - The fat JAR will be in `build/libs/lambda-event-fastq-all.jar`.

## Updating the Code

- Edit `src/main/java/io/nexus/streamlets/functions/FastqGzipIndexingLambda.java` as needed.
- Rebuild the JAR after changes:
  ```sh
  ./gradlew :lambda-event-fastq:shadowJar
  ```

## Running Locally

You can run the main method for local testing (requires S3 access):

```sh
./gradlew :lambda-event-fastq:run
```

Or, run the JAR directly (replace `<BUCKET>` and `<KEY>` as needed):

```sh
java -cp build/libs/lambda-event-fastq-all.jar io.nexus.streamlets.functions.FastqGzipIndexingLambda
```

Edit the `main` method in `FastqGzipIndexingLambda.java` to set your S3 bucket and key.

## Deploying to AWS Lambda

1. Upload the fat JAR (`lambda-event-fastq-all.jar`) to AWS Lambda as a Java function.
2. Set the handler to:
   ```
   io.nexus.streamlets.functions.FastqGzipIndexingLambda::handleRequest
   ```
3. Ensure the Lambda has permissions to read from the source S3 bucket and write to the destination bucket.
4. Set AWS Lambda trigger to invoke the function when a new FASTQ file is uploaded to the source S3 bucket.

## S3 Upload Script

Use `putObject.sh` to upload files to the S3 source bucket:

```sh
sh putObject.sh <file_path> [s3_bucket] [s3_key]
```
- Example:
  ```sh
  sh putObject.sh /path/to/file.fastq nexus-lambda-events myfile.fastq
  ```
- Output includes elapsed time and a JSON timing file.

## Troubleshooting

- Ensure AWS credentials are set up for both CLI and Lambda.
- If you see blocking or deadlocks, check S3 permissions and network connectivity.
- For large files, ensure Lambda has enough memory and timeout.



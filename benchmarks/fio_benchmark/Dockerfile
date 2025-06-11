FROM ubuntu:20.04

RUN apt-get update && apt-get install -y \
    build-essential \             
    fio \
    libcurl4-openssl-dev \
    librados2 \
    libradosstriper1 \
    libfuse-dev \
    libaio-dev \
    libssl-dev \
    curl \
    make \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/axboe/fio.git /fio

WORKDIR /fio

RUN ./configure
RUN make 
RUN make install

COPY fio-benchmark.sh /fio-benchmark.sh
RUN chmod +x /fio-benchmark.sh

ENV \
    FIO_HOST="minio" \
    FIO_PORT=9000 \
    IDENTITY="minioadmin" \
    CREDENTIALS="minioadmin" \
    BUCKET_NAME="test-bucket" \
    BLOB_NAME="k/e.y" \
    HTTP_VERBOSE= \
    #For reference: https://fio.readthedocs.io/en/latest/fio_doc.html#cmdoption-arg-blocksize
    BLOCK_SIZE="4k" \ 
    #For reference: https://fio.readthedocs.io/en/latest/fio_doc.html#cmdoption-arg-size
    FILE_SIZE="64k" \
    #For reference: https://fio.readthedocs.io/en/latest/fio_doc.html#cmdoption-arg-io-size
    IO_SIZE="4k"\
    #Optional for triggering Read verifications from object storage after writes
    VERIFY="sha256"

ENTRYPOINT ["/fio-benchmark.sh"]
#!/bin/bash

cat << EOF > /tmp/fio-test.fio
[global]
ioengine=http
http_mode=s3
http_host=${FIO_HOST}:${FIO_PORT}
http_s3_key=${IDENTITY}
http_s3_keyid=${CREDENTIALS}
filename=/${BUCKET_NAME}/${BLOB_NAME}
direct=1
http_verbose=${HTTP_VERBOSE}
group_reporting

[create]
rw=write
bs=${BLOCK_SIZE}
size=${FILE_SIZE}
io_size=${IO_SIZE}

EOF

if [ "${VERIFY}" ]; then
    cat << EOF >> /tmp/fio-test.fio
verify=${VERIFY}
EOF
fi 

fio /tmp/fio-test.fio & tail -F /dev/


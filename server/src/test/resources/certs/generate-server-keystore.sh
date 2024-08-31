#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# A utility script to re-generate the server keystores used for testing

SCRIPT_DIR=$( dirname -- "$( readlink -f -- "$0"; )"; )
CERT_VALIDITY_DAYS=${CERT_VALIDITY_DAYS:-1825}
FILE_NAME=${FILE_NAME:-server_keystore}

# Generate server key
openssl req -new -multivalue-rdn -keyout "${SCRIPT_DIR}"/"${FILE_NAME}".key -nodes -newkey rsa:2048 \
  -subj "/CN=cs.local/O=Apache Cassandra/L=San Jose/ST=California/C=US" \
  -out "${SCRIPT_DIR}"/"${FILE_NAME}".csr

# Sign the server key with the root CA key
openssl x509 -req -CAkey "${SCRIPT_DIR}"/rootCA.key -CA "${SCRIPT_DIR}"/rootCA.pem \
  -days "${CERT_VALIDITY_DAYS}" -set_serial $RANDOM -sha512 -out "${SCRIPT_DIR}"/"${FILE_NAME}".pem -in "${SCRIPT_DIR}"/"${FILE_NAME}".csr \
  -extfile <(printf "subjectAltName=DNS:localhost,DNS:127.0.0.1,DNS:::1,DNS:cs.local")

# Chain server certs
cat "${SCRIPT_DIR}"/"${FILE_NAME}".pem "${SCRIPT_DIR}"/rootCA.pem >"${SCRIPT_DIR}"/"${FILE_NAME}"_chain.pem

# Generate the server keystore
openssl pkcs12 -export -out "${SCRIPT_DIR}"/"${FILE_NAME}".p12 -in "${SCRIPT_DIR}"/"${FILE_NAME}"_chain.pem \
  -inkey "${SCRIPT_DIR}"/"${FILE_NAME}".key -name "${FILE_NAME}" \
  -passin pass:"password" -passout pass:"password"

# Generate JKS server keystore
keytool -importkeystore -srckeystore "${SCRIPT_DIR}"/"${FILE_NAME}".p12 -srcstoretype pkcs12 \
  -srcalias "${FILE_NAME}" -destkeystore "${SCRIPT_DIR}"/"${FILE_NAME}".jks -deststoretype jks \
  -deststorepass "password" -destalias "${FILE_NAME}" -srcstorepass "password"

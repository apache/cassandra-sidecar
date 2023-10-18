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

# A utility script to re-generate the certificates used for testing
# the password is password

set -xe
SCRIPT_DIR=$( dirname -- "$( readlink -f -- "$0"; )"; )

# clean up existing truststore files
rm -f "${SCRIPT_DIR}"/truststore.p12 "${SCRIPT_DIR}"/truststore.jks "${SCRIPT_DIR}"/server_keystore.p12 \
  "${SCRIPT_DIR}"/server_keystore.jks "${SCRIPT_DIR}"/client_keystore.p12 "${SCRIPT_DIR}"/client_keystore.jks \
  "${SCRIPT_DIR}"/expired_server_keystore.p12

# Create root CA key and PEM file
openssl req -new -x509 -days 1825 -nodes -newkey rsa:2048 -sha512 \
  -out "${SCRIPT_DIR}"/rootCA.pem -keyout "${SCRIPT_DIR}"/rootCA.key \
  -subj "/CN=cs.local/O=Apache Cassandra/L=San Jose/ST=California/C=US"

# Generate PKCS12 truststore with root CA
keytool -keystore "${SCRIPT_DIR}"/truststore.p12 \
  -alias fakerootca \
  -importcert -noprompt \
  -file "${SCRIPT_DIR}"/rootCA.pem \
  -keypass "password" \
  -storepass "password"

# Generate JKS truststore
keytool -importkeystore -srckeystore "${SCRIPT_DIR}"/truststore.p12 -srcstoretype pkcs12 \
  -srcalias fakerootca -destkeystore "${SCRIPT_DIR}"/truststore.jks -deststoretype jks \
  -deststorepass "password" -destalias fakerootca -srcstorepass "password"

# Generate the server keystore
"${SCRIPT_DIR}"/generate-server-keystore.sh

# Generate the client keystore
"${SCRIPT_DIR}"/generate-client-keystore.sh

# Generate the expired server keystore
CERT_VALIDITY_DAYS=-1 FILE_NAME=expired_server_keystore "${SCRIPT_DIR}"/generate-server-keystore.sh

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

set -xe

export CASSANDRA_USE_JDK11=true
SCRIPT_DIR=$( dirname -- "$( readlink -f -- "$0"; )"; )
TARBALL_DIR="$(dirname "${SCRIPT_DIR}/")/cassandra-tarballs"

ant artifacts -Dant.gen-doc.skip=true -Dno-checkstyle=true -Drat.skip=true
mkdir -p ${TARBALL_DIR}
cp ./build/apache-cassandra-*-bin.tar.gz ${TARBALL_DIR}

set +xe

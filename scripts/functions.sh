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

get_hostname() {
  if [[ $# -eq 0 ]] ; then
      echo 'An URI must be provided'
      exit 1
  fi

  # removes the scheme portion of the URI (i.e. https://, http://)
  hostname=$(echo "${1}" | sed -E 's/^\s*.*:\/\///g')
  # removes the first encounter of a forward slash / until the end of the string
  hostname=${hostname%%/*}
  # removes the first encounter of a colon : until the end of the string i.e git@github.com:apache -> git@github.com
  hostname=${hostname%%:*}
  # removes the username portion of an URI i.e. git@github.com -> github.com
  hostname=$(echo "${hostname}" | cut -d@ -f2-)

  echo "${hostname}"
}

test_get_hostname() {
  get_hostname "https://github.com/apache/cassandra-sidecar" # returns github.com
  get_hostname "git@github.com:apache/cassandra-sidecar.git" # returns github.com
  get_hostname # returns An URI must be provided message with exit code 1
}

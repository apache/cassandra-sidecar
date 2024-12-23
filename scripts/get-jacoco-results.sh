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

search_dir="."

target_folder="${1}"

mkdir -p $target_folder

find "$search_dir" -type d -path "*/build/jacoco" -print0 | while IFS= read -r -d '' dir; do
  destination_path="$target_folder/${dir:2}"
  mkdir -p "$destination_path"
  cp -r $dir "$destination_path"
  echo "Copied folder $dir to $destination_path"
done


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
# Usage: ./publish-ecr.sh [-n <namespace>]
#   -n  ECR namespace prefix (optional). When provided, the image is pushed as
#       <namespace>/cassandra-sidecar. Without it, the image is cassandra-sidecar.
#       Can also be set via REPO_NAMESPACE env var. Flag takes precedence.
#
# Prerequisites:
#   1. The ECR repository will be created automatically if it does not exist.
#
#   2. The AWS identity must have the following IAM permissions:
#        sts:GetCallerIdentity
#        ecr:GetAuthorizationToken
#        ecr:BatchCheckLayerAvailability
#        ecr:InitiateLayerUpload
#        ecr:UploadLayerPart
#        ecr:CompleteLayerUpload
#        ecr:PutImage
#      The AWS managed policy AmazonEC2ContainerRegistryPowerUser grants all of these.

set -euo pipefail

export AWS_PAGER=""

REGION="us-west-2"
REPO_NAMESPACE="${REPO_NAMESPACE:-}"

while getopts "n:" opt; do
    case "${opt}" in
        n) REPO_NAMESPACE="${OPTARG}" ;;
        *) echo "Usage: $0 [-n <namespace>]"; exit 1 ;;
    esac
done

REPO_NAME="${REPO_NAMESPACE:+${REPO_NAMESPACE}/}cassandra-sidecar"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGISTRY="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
IMAGE="${REGISTRY}/${REPO_NAME}"

if ! aws ecr describe-repositories --repository-names "${REPO_NAME}" --region "${REGION}" &>/dev/null; then
    echo "ECR repository '${REPO_NAME}' does not exist in ${REGISTRY}."
    read -r -p "Create it now? [y/N] " answer
    case "${answer}" in
        [yY]) aws ecr create-repository --repository-name "${REPO_NAME}" --region "${REGION}" ;;
        *) echo "Aborting."; exit 1 ;;
    esac
fi

PASSWORD=$(aws ecr get-login-password --region "${REGION}")

SCRIPT_DIR=$(dirname -- "$(readlink -f -- "$0")")
cd "${SCRIPT_DIR}/.."

./gradlew :server:jib \
  -Djib.to.image="${IMAGE}" \
  -Djib.to.auth.username=AWS \
  "-Djib.to.auth.password=${PASSWORD}"

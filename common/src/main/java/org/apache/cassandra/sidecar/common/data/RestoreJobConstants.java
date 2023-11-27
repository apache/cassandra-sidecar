/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.common.data;

/**
 * Holds restore job API requests related constants.
 */
public class RestoreJobConstants
{
    public static final String JOB_ID = "jobId";
    public static final String JOB_AGENT = "jobAgent";
    public static final String JOB_STATUS = "status";
    public static final String JOB_SECRETS = "secrets";
    public static final String JOB_EXPIRE_AT = "expireAt";
    public static final String JOB_IMPORT_OPTIONS = "importOptions";
    public static final String JOB_CREATED_AT = "createdAt";
    public static final String JOB_KEYSPACE = "keyspace";
    public static final String JOB_TABLE = "table";
    public static final String SLICE_ID = "sliceId";
    public static final String BUCKET_ID = "bucketId";
    public static final String SLICE_START_TOKEN = "startToken";
    public static final String SLICE_END_TOKEN = "endToken";
    public static final String SLICE_STORAGE_BUCKET = "storageBucket";
    public static final String SLICE_STORAGE_KEY = "storageKey";
    public static final String SLICE_CHECKSUM = "sliceChecksum";
    public static final String SLICE_UNCOMPRESSED_SIZE = "sliceUncompressedSize";
    public static final String SLICE_COMPRESSED_SIZE = "sliceCompressedSize";
    public static final String SECRET_READ_CREDENTIALS = "readCredentials";
    public static final String SECRET_WRITE_CREDENTIALS = "writeCredentials";
    public static final String CREDENTIALS_ACCESS_KEY_ID = "accessKeyId";
    public static final String CREDENTIALS_SECRET_ACCESS_KEY = "secretAccessKey";
    public static final String CREDENTIALS_SESSION_TOKEN = "sessionToken";
    public static final String CREDENTIALS_REGION = "region";
}

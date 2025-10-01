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

package org.apache.cassandra.sidecar.job;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.common.server.StorageOperations;

/**
 * Implementation of {@link OperationalJob} to perform node flush operation.
 */
public class NodeFlushJob extends OperationalJob
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeFlushJob.class);
    private static final String OPERATION = "flush";
    private final String keyspace;
    private final List<String> tableNames;
    protected StorageOperations storageOperations;

    public NodeFlushJob(UUID jobId, StorageOperations storageOps, String keyspace, List<String> tableNames)
    {
        super(jobId);
        this.storageOperations = storageOps;
        this.keyspace = keyspace;
        this.tableNames = tableNames;
    }

    @Override
    public boolean isRunningOnCassandra()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void executeInternal() throws IOException
    {
        LOGGER.info("Executing flush operation for keyspace={}, tables={}. jobId={}", keyspace, tableNames, this.jobId());

        String[] tableArray = tableNames != null ? tableNames.toArray(new String[0]) : new String[0];
        storageOperations.flush(keyspace, tableArray);

        LOGGER.info("Flush operation completed for keyspace={}, tables={}. jobId={}", keyspace, tableNames, this.jobId());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String name()
    {
        return OPERATION;
    }
}

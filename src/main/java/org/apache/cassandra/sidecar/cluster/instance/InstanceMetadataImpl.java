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

package org.apache.cassandra.sidecar.cluster.instance;

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.sidecar.common.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.CassandraVersionProvider;
import org.apache.cassandra.sidecar.common.JmxClient;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Local implementation of InstanceMetadata.
 */
public class InstanceMetadataImpl implements InstanceMetadata
{
    private final int id;
    private final String host;
    private final int port;
    private final List<String> dataDirs;
    private final String uploadsStagingDir;
    private final CassandraAdapterDelegate delegate;

    public InstanceMetadataImpl(int id,
                                String host,
                                int port,
                                Iterable<String> dataDirs,
                                String uploadsStagingDir,
                                CQLSessionProvider sessionProvider,
                                JmxClient jmxClient,
                                CassandraVersionProvider versionProvider)
    {
        this(id, host, port, dataDirs, uploadsStagingDir,
             new CassandraAdapterDelegate(versionProvider, sessionProvider, jmxClient));
    }

    @VisibleForTesting
    public InstanceMetadataImpl(int id,
                                String host,
                                int port,
                                Iterable<String> dataDirs,
                                String uploadsStagingDir,
                                CassandraAdapterDelegate delegate)
    {
        this.id = id;
        this.host = host;
        this.port = port;
        this.dataDirs = ImmutableList.copyOf(dataDirs);
        this.uploadsStagingDir = uploadsStagingDir;
        this.delegate = delegate;
    }

    @Override
    public int id()
    {
        return id;
    }

    @Override
    public String host()
    {
        return host;
    }

    @Override
    public int port()
    {
        return port;
    }

    @Override
    public List<String> dataDirs()
    {
        return dataDirs;
    }

    @Override
    public String uploadsStagingDir()
    {
        return uploadsStagingDir;
    }

    @Override
    public CassandraAdapterDelegate delegate()
    {
        return delegate;
    }
}

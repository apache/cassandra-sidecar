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

import org.apache.cassandra.sidecar.common.CQLSession;
import org.apache.cassandra.sidecar.common.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.CassandraVersionProvider;
import org.apache.cassandra.sidecar.common.JmxClient;

/**
 * Local implementation of InstanceMetadata.
 */
public class InstanceMetadataImpl implements InstanceMetadata
{
    private final int id;
    private final List<String> dataDirs;
    private final CQLSession session;
    private final JmxClient jmxClient;
    private final CassandraAdapterDelegate delegate;

    public InstanceMetadataImpl(int id,
                                List<String> dataDirs,
                                CQLSession session,
                                JmxClient jmxClient,
                                CassandraVersionProvider versionProvider)
    {
        this.id = id;
        this.dataDirs = dataDirs;

        this.session = session;
        this.jmxClient = jmxClient;
        this.delegate = new CassandraAdapterDelegate(versionProvider, session, jmxClient);
    }

    @Override
    public int id()
    {
        return id;
    }

    @Override
    public String host()
    {
        return session.inet().getHostName();
    }

    @Override
    public int port()
    {
        return session.inet().getPort();
    }

    @Override
    public List<String> dataDirs()
    {
        return dataDirs;
    }

    @Override
    public CQLSession session()
    {
        return session;
    }

    @Override
    public JmxClient jmxClient()
    {
        return jmxClient;
    }

    @Override
    public CassandraAdapterDelegate delegate()
    {
        return delegate;
    }
}

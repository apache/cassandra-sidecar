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

package org.apache.cassandra.sidecar.common.testing;

import org.apache.cassandra.sidecar.common.CQLSession;
import org.apache.cassandra.sidecar.common.ICassandraAdapter;
import org.apache.cassandra.sidecar.common.JmxClient;
import org.apache.cassandra.sidecar.common.SimpleCassandraVersion;
import org.apache.cassandra.sidecar.common.containers.CassandraContainer;

/**
 * Passed to integration tests.
 * See {@link CassandraIntegrationTest} for the required annotation
 * See {@link CassandraTestTemplate} for the Test Template
 */
public class CassandraTestContext
{
    public final JmxClient jmxClient;
    public final CQLSession session;
    public final SimpleCassandraVersion version;
    public final CassandraContainer container;
    public final ICassandraAdapter cassandra;

    CassandraTestContext(SimpleCassandraVersion version,
                         CassandraContainer container,
                         CQLSession session,
                         JmxClient jmxClient,
                         ICassandraAdapter cassandra)
    {
        this.version = version;
        this.container = container;
        this.session = session;
        this.cassandra = cassandra;
        this.jmxClient = jmxClient;
    }

    @Override
    public String toString()
    {
        return "CassandraTestContext{" +
               "jmxClient=" + jmxClient +
               ", session=" + session +
               ", version=" + version +
               ", container=" + container +
               ", cassandra=" + cassandra +
               '}';
    }
}

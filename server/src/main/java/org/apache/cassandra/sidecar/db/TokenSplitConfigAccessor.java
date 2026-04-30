/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.db;

import com.google.inject.Inject;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;

/**
 * Configuration accessor for token splitting functionality in Cassandra Sidecar.
 * <p>
 * This class extends {@link ConfigAccessorImpl} to provide specialized configuration management
 * for token splitting operations. Token splitting is a critical component of CDC (Change Data Capture)
 * and distributed processing, enabling:
 * <ul>
 *   <li>Partitioning of Cassandra token ranges into smaller, manageable splits</li>
 *   <li>Distributed processing coordination across multiple CDC consumers</li>
 *   <li>Scalable data processing by parallelizing work across token range boundaries</li>
 *   <li>Configuration persistence and retrieval for token split parameters</li>
 * </ul>
 * <p>
 * The token split configuration manages parameters such as:
 * <ul>
 *   <li><strong>Split count:</strong> Number of splits to divide token ranges into</li>
 *   <li><strong>Split boundaries:</strong> Specific token range boundaries for each split</li>
 *   <li><strong>Processing coordination:</strong> Configuration for distributed processing</li>
 *   <li><strong>Performance tuning:</strong> Parameters for optimizing split operations</li>
 * </ul>
 * <p>
 * This accessor uses the "tokensplit" service identifier to distinguish its configuration
 * from other service configurations in the sidecar system. It leverages the underlying
 * {@link ConfigAccessorImpl} infrastructure for configuration persistence, caching,
 * and retrieval operations.
 * <p>
 * Token splitting is essential for CDC operations as it allows CDC consumers to process
 * data in parallel across different portions of the token ring, improving throughput
 * and enabling horizontal scaling of CDC workloads.
 * <p>
 * This class is thread-safe and designed as a singleton for dependency injection into
 * components that require token split configuration access.
 *
 * @see ConfigAccessorImpl
 * @see org.apache.cassandra.sidecar.utils.TokenSplitUtil
 * @see CdcDatabaseAccessor
 */
public class TokenSplitConfigAccessor extends ConfigAccessorImpl
{
    @Inject
    protected TokenSplitConfigAccessor(CQLSessionProvider sessionProvider,
                                       SidecarSchema sidecarSchema)
    {
        super(sessionProvider, sidecarSchema);
    }

    @Override
    public String service()
    {
        return "tokensplit";
    }
}

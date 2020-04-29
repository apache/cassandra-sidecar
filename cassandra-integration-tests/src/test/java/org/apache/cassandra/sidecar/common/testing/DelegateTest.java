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

import java.io.IOException;

import io.kubernetes.client.openapi.ApiException;
import org.apache.cassandra.sidecar.common.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.CassandraVersionProvider;
import org.apache.cassandra.sidecar.common.SimpleCassandraVersion;
import org.apache.cassandra.sidecar.mocks.V30;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Insures the Delegate works correctly
 */
public class DelegateTest
{
    @CassandraIntegrationTest
    void testCorrectVersionIsEnabled(CassandraTestContext context)
    {
        CassandraVersionProvider provider = new CassandraVersionProvider.Builder().add(new V30()).build();
        CassandraAdapterDelegate delegate = new CassandraAdapterDelegate(provider, context.session);
        delegate.checkSession();
        SimpleCassandraVersion version = delegate.getVersion();
        assertThat(version).isNotNull();
    }

    @CassandraIntegrationTest
    void testHealthCheck(CassandraTestContext context) throws InterruptedException, ApiException, IOException
    {
        CassandraVersionProvider provider = new CassandraVersionProvider.Builder().add(new V30()).build();
        CassandraAdapterDelegate delegate = new CassandraAdapterDelegate(provider, context.session);

        delegate.checkSession();
        delegate.healthCheck();

        assertThat(delegate.isUp()).isTrue();

        context.container.disableBinary();

        delegate.healthCheck();
        assertThat(delegate.isUp()).isFalse();

        context.container.enableBinary();

        delegate.healthCheck();

        assertThat(delegate.isUp()).isTrue();
    }
}

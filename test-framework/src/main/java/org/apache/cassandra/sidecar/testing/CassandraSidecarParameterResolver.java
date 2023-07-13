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

package org.apache.cassandra.sidecar.testing;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vdurmont.semver4j.Semver;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.sidecar.adapters.base.CassandraFactory;
import org.apache.cassandra.sidecar.common.CassandraVersionProvider;
import org.apache.cassandra.sidecar.common.SimpleCassandraVersion;
import org.apache.cassandra.sidecar.common.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.utils.SidecarVersionProvider;
import org.apache.cassandra.testing.CassandraTestContext;

import static org.apache.cassandra.distributed.test.hostreplacement.NodeCannotJoinAsHibernatingNodeWithoutReplaceAddressTest.SharedState.cluster;


public class CassandraSidecarParameterResolver implements ParameterResolver
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraSidecarParameterResolver.class);
    private static SidecarVersionProvider svp = new SidecarVersionProvider("/sidecar.version");

    CassandraSidecarParameterResolver() {}

    @Override
    public boolean supportsParameter(ParameterContext parameterContext,
                                                      ExtensionContext extensionContext)
    {
        return parameterContext.getParameter().getType().equals(CassandraSidecarTestContext.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
    {
        throw new UnsupportedOperationException("No longer!");
//        CassandraTestContext rootContext =
//            extensionContext.getStore(ExtensionContext.Namespace.create("org.apache.cassandra.testing"))
//                            .get("cassandra_test_context", CassandraTestContext.class);
//        if (rootContext == null) {
//            throw new RuntimeException(this.getClass().getSimpleName() + "requires the use of the @CassandraIntegrationTest annotation");
//        }
//        org.apache.cassandra.testing.SimpleCassandraVersion rootVersion = rootContext.version;
//        logger.info("Testing {} against in-jvm dtest cluster", rootVersion);
//        SimpleCassandraVersion versionParsed = SimpleCassandraVersion.create(rootVersion.major, rootVersion.minor, rootVersion.patch);
//        CassandraVersionProvider versionProvider = cassandraVersionProvider(DnsResolver.DEFAULT);
//        CassandraSidecarTestContext cassandraSidecarTestContext;
//        try
//        {
//            cassandraSidecarTestContext = new CassandraSidecarTestContext(versionParsed, rootContext.cluster, versionProvider);
//        }
//        catch (IOException e)
//        {
//            throw new RuntimeException(e);
//        }
//        logger.info("Created test context {}", cassandraSidecarTestContext);
//        return cassandraSidecarTestContext;
    }


    public CassandraVersionProvider cassandraVersionProvider(DnsResolver dnsResolver)
    {
        return new CassandraVersionProvider.Builder()
               .add(new CassandraFactory(dnsResolver, svp.sidecarVersion())).build();
    }
}

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

package org.apache.cassandra.sidecar;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.WorkerPoolConfiguration;

/**
 * Changes to the TestModule to define SSL dependencies
 */
public class TestSslModule extends TestModule
{
    private static final Logger logger = LoggerFactory.getLogger(TestSslModule.class);

    @Override
    public Configuration abstractConfig(InstancesConfig instancesConfig)
    {
        String keyStorePath = TestSslModule.class.getClassLoader()
                                                 .getResource("certs/test.p12")
                                                 .getPath();
        String keyStorePassword = "password";

        String trustStorePath = TestSslModule.class.getClassLoader()
                                                         .getResource("certs/ca.p12")
                                                         .getPath();
        String trustStorePassword = "password";

        if (!Files.exists(Paths.get(keyStorePath)))
        {
            logger.error("JMX password file not found in path={}", keyStorePath);
        }
        if (!Files.exists(Paths.get(trustStorePath)))
        {
            logger.error("Trust Store file not found in path={}", trustStorePath);
        }

        WorkerPoolConfiguration workerPoolConf = new WorkerPoolConfiguration("test-pool", 10,
                                                                             30000);

        return new Configuration.Builder<>()
               .setInstancesConfig(instancesConfig)
               .setHost("127.0.0.1")
               .setPort(6475)
               .setHealthCheckFrequency(1000)
               .setKeyStorePath(keyStorePath)
               .setKeyStorePassword(keyStorePassword)
               .setTrustStorePath(trustStorePath)
               .setTrustStorePassword(trustStorePassword)
               .setSslEnabled(true)
               .setRateLimitStreamRequestsPerSecond(1)
               .setRequestIdleTimeoutMillis(300_000)
               .setRequestTimeoutMillis(300_000L)
               .setConcurrentUploadsLimit(80)
               .setMinSpacePercentRequiredForUploads(0)
               .setSSTableImportCacheConfiguration(new CacheConfiguration(60_000, 100))
               .setServerWorkerPoolConfiguration(workerPoolConf)
               .setServerInternalWorkerPoolConfiguration(workerPoolConf)
               .build();
    }
}

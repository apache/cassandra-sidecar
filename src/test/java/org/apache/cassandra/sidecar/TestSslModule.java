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

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Changes to the TestModule to define SSL dependencies
 */
public class TestSslModule extends TestModule
{
    private static final Logger logger = LoggerFactory.getLogger(TestSslModule.class);


    @Override
    public Configuration abstractConfig()
    {
        final String keyStorePath = TestSslModule.class.getClassLoader().getResource("certs/test.p12").getPath();
        final String keyStorePassword = "password";

        final String trustStorePath = TestSslModule.class.getClassLoader().getResource("certs/ca.p12").getPath();
        final String trustStorePassword = "password";

        if (!new File(keyStorePath).exists())
        {
            logger.error("JMX password file not found");
        }
        if (!new File(trustStorePath).exists())
        {
            logger.error("Trust Store file not found");
        }

        return new Configuration.Builder()
                           .setInstancesConfig(getInstancesConfig())
                           .setHost("127.0.0.1")
                           .setPort(6475)
                           .setHealthCheckFrequency(1000)
                           .setKeyStorePath(keyStorePath)
                           .setKeyStorePassword(keyStorePassword)
                           .setTrustStorePath(trustStorePath)
                           .setTrustStorePassword(trustStorePassword)
                           .setSslEnabled(true)
                           .setRateLimitStreamRequestsPerSecond(1)
                           .build();
    }
}

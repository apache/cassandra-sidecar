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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test daemon validation and initialization
 */
public class CassandraDaemonTest
{
    @Test
    public void testValidateSSLNotSet()
    {
        Configuration config = new Configuration.Builder()
                               .build();
        new CassandraSidecarDaemon(null, null, config).validate();
    }

    @Test
    public void testValidateSSLNotSetButEnabled()
    {
        Configuration config = new Configuration.Builder()
                               .withSslEnabled(true)
                               .build();
        assertThrows(RuntimeException.class, () -> new CassandraSidecarDaemon(null, null, config)
                                                   .validate());
    }

    @Test
    public void testValidateSSLMissing()
    {
        final Configuration justKeystore = new Configuration.Builder()
                                           .withSslEnabled(true)
                                           .withKeyStorePath("noneexistant")
                                           .build();
        assertThrows(RuntimeException.class, () -> new CassandraSidecarDaemon(null, null, justKeystore)
                                                   .validate());

        final Configuration justPassword = new Configuration.Builder()
                                           .withSslEnabled(true)
                                           .withKeyStorePassword("password")
                                           .build();
        assertThrows(RuntimeException.class, () -> new CassandraSidecarDaemon(null, null, justPassword)
                                                   .validate());


        final Configuration missing = new Configuration.Builder()
                                      .withSslEnabled(true)
                                      .withKeyStorePath("noneexistant.p12")
                                      .withKeyStorePassword("password")
                                      .build();
        assertThrows(RuntimeException.class, () -> new CassandraSidecarDaemon(null, null, missing)
                                                   .validate());

        final Configuration jks = new Configuration.Builder()
                                      .withSslEnabled(true)
                                      .withKeyStorePath("noneexistant.jks")
                                      .withKeyStorePassword("password")
                                      .build();
        assertThrows(RuntimeException.class, () -> new CassandraSidecarDaemon(null, null, jks)
                                                   .validate());

        final Configuration notype = new Configuration.Builder()
                                  .withSslEnabled(true)
                                  .withKeyStorePath("noneexistant.blarg")
                                  .withKeyStorePassword("password")
                                  .build();
        assertThrows(RuntimeException.class, () -> new CassandraSidecarDaemon(null, null, notype)
                                                   .validate());
    }

    @Test
    public void testValidateSSLCorrect()
    {
        final String trustStorePath = CassandraDaemonTest.class.getClassLoader()
                                                               .getResource("certs/ca.p12")
                                                               .getPath();
        final String trustStorePassword = "password";

        final Configuration valid = new Configuration.Builder()
                                    .withSslEnabled(true)
                                    .withKeyStorePath(trustStorePath)
                                    .withKeyStorePassword(trustStorePassword)
                                    .build();

        new CassandraSidecarDaemon(null, null, valid).validate();
    }
}

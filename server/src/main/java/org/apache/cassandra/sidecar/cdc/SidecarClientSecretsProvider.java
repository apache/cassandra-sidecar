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

package org.apache.cassandra.sidecar.cdc;

import java.util.Map;
import java.util.TreeMap;

import org.apache.cassandra.secrets.SslConfig;
import org.apache.cassandra.secrets.SslConfigSecretsProvider;
import org.apache.cassandra.sidecar.config.KeyStoreConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;

/**
 * A {@link SslConfigSecretsProvider} that reads SSL configuration from {@link SidecarConfiguration}
 * to provide keystore and truststore secrets for the sidecar client.
 */
public class SidecarClientSecretsProvider extends SslConfigSecretsProvider
{
    public SidecarClientSecretsProvider(SidecarConfiguration sidecarConfiguration)
    {
        super(SslConfig.create(buildSslConfigMap(sidecarConfiguration)));
    }

    private static Map<String, String> buildSslConfigMap(SidecarConfiguration sidecarConfiguration)
    {
        SslConfiguration sslConfiguration = sidecarConfiguration.sidecarClientConfiguration().sslConfiguration();

        Map<String, String> sslConfigMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        if (sslConfiguration.isKeystoreConfigured())
        {
            KeyStoreConfiguration keystore = sslConfiguration.keystore();
            sslConfigMap.put(SslConfig.KEYSTORE_PATH, keystore.path());
            sslConfigMap.put(SslConfig.KEYSTORE_PASSWORD, keystore.password());
            sslConfigMap.put(SslConfig.KEYSTORE_TYPE, keystore.type());
        }

        if (sslConfiguration.isTrustStoreConfigured())
        {
            KeyStoreConfiguration truststore = sslConfiguration.truststore();
            sslConfigMap.put(SslConfig.TRUSTSTORE_PATH, truststore.path());
            sslConfigMap.put(SslConfig.TRUSTSTORE_PASSWORD, truststore.password());
            sslConfigMap.put(SslConfig.TRUSTSTORE_TYPE, truststore.type());
        }

        return sslConfigMap;
    }
}

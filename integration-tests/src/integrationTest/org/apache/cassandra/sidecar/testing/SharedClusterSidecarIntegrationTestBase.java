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

import io.vertx.core.net.JksOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;

import static io.vertx.core.Vertx.vertx;
import static org.apache.cassandra.sidecar.testing.MtlsTestHelper.EMPTY_PASSWORD_STRING;

/**
 * Builds on top of {@link SharedClusterIntegrationTestBase} and adds functionality to interact
 * with the Sidecar process with a trusted client
 */
public abstract class SharedClusterSidecarIntegrationTestBase extends SharedClusterIntegrationTestBase
{
    private WebClient trustedClient;
    private WebClient noAuthClient;

    @Override
    protected void beforeClusterShutdown()
    {
        super.beforeClusterShutdown();

        if (trustedClient != null)
        {
            trustedClient.close();
        }

        if (noAuthClient != null)
        {
            noAuthClient.close();
        }
    }

    /**
     * @return a client that configures the truststore and the client keystore
     */
    public WebClient trustedClient()
    {
        if (trustedClient != null)
        {
            return trustedClient;
        }

        WebClientOptions clientOptions = new WebClientOptions()
                                         .setKeyStoreOptions(new JksOptions()
                                                             .setPath(mtlsTestHelper.clientKeyStorePath())
                                                             .setPassword(EMPTY_PASSWORD_STRING))
                                         .setTrustStoreOptions(new JksOptions()
                                                               .setPath(mtlsTestHelper.trustStorePath())
                                                               .setPassword(EMPTY_PASSWORD_STRING))
                                         .setSsl(true);
        trustedClient = WebClient.create(vertx(), clientOptions);
        return trustedClient;
    }

    /**
     * @return a client that configures the truststore, but does not provide a client identity
     */
    public WebClient noAuthClient()
    {
        if (noAuthClient != null)
        {
            return noAuthClient;
        }

        WebClientOptions clientOptions = new WebClientOptions()
                                         .setTrustStoreOptions(new JksOptions()
                                                               .setPath(mtlsTestHelper.trustStorePath())
                                                               .setPassword(mtlsTestHelper.trustStorePassword()))
                                         .setSsl(true);
        noAuthClient = WebClient.create(vertx(), clientOptions);
        return noAuthClient;
    }
}

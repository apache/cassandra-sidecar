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

import java.util.Objects;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;

import static io.vertx.core.Vertx.vertx;
import static org.apache.cassandra.sidecar.testing.MtlsTestHelper.PASSWORD_STRING;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Builds on top of {@link SharedClusterIntegrationTestBase} and adds functionality to interact
 * with the Sidecar process with a trusted client
 */
public abstract class SharedClusterSidecarIntegrationTestBase extends SharedClusterIntegrationTestBase
{
    private WebClient trustedClient;
    private WebClient noAuthClient;

    /**
     * A utility functional interface to perform verifications for a test
     *
     * @param <T> the type accepted by the verifier
     */
    @FunctionalInterface
    public interface Verifier<T>
    {
        void accept(T v) throws AssertionError;

        default Verifier<T> andThen(Verifier<T> after)
        {
            Objects.requireNonNull(after);

            return v -> {
                accept(v);
                after.accept(v);
            };
        }
    }

    protected static Verifier<HttpResponse<Buffer>> assertStatus(HttpResponseStatus expectedStatus)
    {
        return response -> {
            assertThat(response).isNotNull();
            assertThat(response.statusCode()).isEqualTo(expectedStatus.code());
        };
    }

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
        if (trustedClient == null)
        {
            trustedClient = trustedClient(mtlsTestHelper.clientKeyStorePath(), PASSWORD_STRING,
                                          mtlsTestHelper.trustStorePath(), PASSWORD_STRING);
        }
        return trustedClient;
    }

    /**
     * @param clientKeyStorePath     the path to the client keyStore
     * @param clientKeyStorePassword the password for the client keyStore
     * @param trustStorePath         the path to the trustStore
     * @param trustStorePassword     the password for the trustStore
     * @return a client that configures the truststore and the client keystore
     */
    public WebClient trustedClient(String clientKeyStorePath, String clientKeyStorePassword, String trustStorePath, String trustStorePassword)
    {
        WebClientOptions clientOptions = new WebClientOptions()
                                         .setKeyStoreOptions(new JksOptions()
                                                             .setPath(clientKeyStorePath)
                                                             .setPassword(clientKeyStorePassword))
                                         .setTrustStoreOptions(new JksOptions()
                                                               .setPath(trustStorePath)
                                                               .setPassword(trustStorePassword))
                                         .setSsl(true);
        return WebClient.create(vertx(), clientOptions);
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

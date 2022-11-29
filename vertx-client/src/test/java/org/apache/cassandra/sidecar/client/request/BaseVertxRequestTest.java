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

package org.apache.cassandra.sidecar.client.request;

import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.client.HttpClientConfig;
import org.apache.cassandra.sidecar.client.RequestExecutor;
import org.apache.cassandra.sidecar.client.VertxHttpClient;
import org.apache.cassandra.sidecar.client.VertxRequestExecutor;
import org.apache.cassandra.sidecar.client.retry.BasicRetryPolicy;
import org.apache.cassandra.sidecar.client.retry.RetryPolicy;

/**
 * Base test for running all tests inside the RequestExecutor test using the vertx client
 *
 * @param <T> the type of request
 */
public class BaseVertxRequestTest<T> extends RequestExecutorTest<T>
{
    final Vertx vertx = Vertx.vertx();

    @Override
    protected RequestExecutor sidecarClient()
    {
        return new VertxRequestExecutor(this.httpClient());
    }

    @Override
    protected VertxHttpClient httpClient()
    {
        HttpClientConfig config = httpClientConfig();
        return new VertxHttpClient(vertx, config);
    }

    @Override
    protected RetryPolicy retryPolicy()
    {
        return new BasicRetryPolicy(3, 200);
    }

    @Override
    protected HttpClientConfig httpClientConfig()
    {
        return httpClientConfigBuilder().build();
    }
}

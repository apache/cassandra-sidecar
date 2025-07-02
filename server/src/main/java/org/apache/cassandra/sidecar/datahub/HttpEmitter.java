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

package org.apache.cassandra.sidecar.datahub;

import datahub.client.Emitter;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import org.apache.cassandra.sidecar.config.SchemaReportingConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.jetbrains.annotations.NotNull;

/**
 * A simple implementation of DataHub {@link Emitter} interface that allows reporting converted schema to the configured HTTP endpoint as a batch of aspects in
 * JSON format
 */
@SuppressWarnings("unused")
public class HttpEmitter extends JsonEmitter
{
    @NotNull
    protected final HttpClient client;
    @NotNull
    protected final String endpoint;
    @NotNull
    protected final HttpMethod method;

    public HttpEmitter(@NotNull Vertx vertx,
                       @NotNull SidecarConfiguration sidecarConfiguration)
    {
        SchemaReportingConfiguration reportingConfiguration = sidecarConfiguration.schemaReportingConfiguration();

        this.client = vertx.createHttpClient();
        this.endpoint = reportingConfiguration.endpoint();
        this.method = reportingConfiguration.method();
    }

    @Override
    public synchronized void close()
    {
        super.close();

        String content = content();

        client.request(method, endpoint)
              .onSuccess(request -> {
                  request.putHeader("Content-Type", "Application/JSON");
                  request.putHeader("Content-Length", Integer.toString(content.length()));
                  request.write(content);
                  request.end();
              });
    }
}

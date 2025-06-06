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

import java.util.List;
import java.util.Objects;

import com.google.inject.AbstractModule;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.exceptions.NoSuchCassandraInstanceException;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Collection of Guice modules helpful for unit testing.
 */
public class HelperTestModules
{
    /**
     * Test module for mocking Vertx routing context.
     */
    public static class RoutingContextTestModule extends AbstractModule
    {
        RoutingContext mockRoutingContext = mock(RoutingContext.class);
        HttpServerRequest httpServerRequest = mock(HttpServerRequest.class);
        HttpServerResponse httpServerResponse = mock(HttpServerResponse.class);
        SocketAddress socketAddress = mock(SocketAddress.class);

        @Override
        protected void configure()
        {
            bind(SocketAddress.class).toInstance(socketAddress);
            bind(HttpServerRequest.class).toInstance(httpServerRequest);
            bind(RoutingContext.class).toInstance(mockRoutingContext);
            bind(HttpServerResponse.class).toInstance(httpServerResponse);

            when(httpServerResponse.setStatusCode(anyInt())).thenReturn(httpServerResponse);
            when(httpServerResponse.putHeader(anyString(), anyString())).thenReturn(httpServerResponse);
            when(httpServerRequest.localAddress()).thenReturn(socketAddress);

            when(mockRoutingContext.request()).thenReturn(httpServerRequest);
            when(mockRoutingContext.response()).thenReturn(httpServerResponse);
        }
    }

    /**
     * Helper module for mocking {@link InstanceMetadata} for unit tests.
     */
    public static class InstanceMetadataTestModule extends AbstractModule
    {

        private final List<InstanceMetadata> instanceMetadataList;

        public InstanceMetadataTestModule(List<InstanceMetadata> instanceMetadataList)
        {
            this.instanceMetadataList = instanceMetadataList;
        }

        protected void configure()
        {
            InstancesMetadata mockInstancesMetadata = mock(InstancesMetadata.class);
            when(mockInstancesMetadata.instances()).thenReturn(instanceMetadataList);
            when(mockInstancesMetadata.instanceFromHost(anyString()))
            .thenAnswer(invocation -> instanceMetadataList.stream()
                                                          .filter(instanceMetadata -> Objects.equals(instanceMetadata.host(),
                                                                                                     invocation.getArgument(0)))
                                                          .findFirst()
                                                          .orElseThrow(() -> new NoSuchCassandraInstanceException("Instance does not exist")));
            when(mockInstancesMetadata.instanceFromId(anyInt()))
            .thenAnswer(invocation -> instanceMetadataList.stream()
                                                          .filter(instanceMetadata -> invocation.getArgument(0).equals(instanceMetadata.id()))
                                                          .findFirst()
                                                          .orElseThrow(() -> new NoSuchCassandraInstanceException("No Cassandra instance exists with given ID")));

            bind(InstancesMetadata.class).toInstance(mockInstancesMetadata);
        }
    }
}

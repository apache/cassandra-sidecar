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

package org.apache.cassandra.testing;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.cassandra.distributed.api.IInstance;

import static org.apache.cassandra.testing.utils.IInstanceUtils.tryGetIntConfig;

/**
 * Test utility class for the Cassandra driver
 */
public class DriverTestUtils
{
    public static List<InetSocketAddress> buildContactPoints(Iterable<? extends IInstance> instances)
    {
        return StreamSupport.stream(instances.spliterator(), false)
                            .map(instance -> new InetSocketAddress(instance.config().broadcastAddress().getAddress(),
                                                                   tryGetIntConfig(instance, "native_transport_port", 9042)))
                            .collect(Collectors.toList());
    }
}

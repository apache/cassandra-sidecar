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

package org.apache.cassandra.sidecar.common;

/**
 * A factory is used here to create instances of an Adapter.  We
 */
public interface ICassandraFactory
{
    /**
     * Creates a new {@link ICassandraAdapter} with the provided {@link CQLSession} and {@link JmxClient}
     *
     * @param session the session to the Cassandra database
     * @param client  the JMX client to connect to the Cassandra database
     * @return a {@link ICassandraAdapter}
     */
    ICassandraAdapter create(CQLSession session, JmxClient client);
}

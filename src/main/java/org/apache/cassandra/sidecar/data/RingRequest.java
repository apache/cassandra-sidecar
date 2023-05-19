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

package org.apache.cassandra.sidecar.data;

import java.util.Objects;

/**
 * Holder class for the {@link org.apache.cassandra.sidecar.routes.RingHandler}
 * request parameters
 */
public class RingRequest
{
    private final String keyspace;

    /**
     * Constructs a ring request
     */
    public RingRequest()
    {
        this.keyspace = null;
    }

    /**
     * Constructs a ring request with an optional {@code keyspace} and a {@code resolveIp} parameter.
     *
     * @param keyspace  the keyspace in Cassandra
     */
    public RingRequest(String keyspace)
    {
        this.keyspace = keyspace;
    }

    /**
     * @return the keyspace for the request
     */
    public String keyspace()
    {
        return keyspace;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RingRequest that = (RingRequest) o;
        return Objects.equals(keyspace, that.keyspace);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(keyspace);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "RingRequest{" +
               "keyspace='" + keyspace +
               "'}";
    }
}

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

package org.apache.cassandra.sidecar.client;

import java.util.Objects;

/**
 * A simple implementation of the {@link SidecarInstance} interface
 */
public class SidecarInstanceImpl implements SidecarInstance
{
    protected int port;
    protected String hostname;

    /**
     * Constructs a new Sidecar instance with the given {@code port} and {@code hostname}
     *
     * @param hostname the host name where Sidecar is running
     * @param port     the port where Sidecar is running
     */
    public SidecarInstanceImpl(String hostname, int port)
    {
        if (port < 1 || port > 65535)
        {
            throw new IllegalArgumentException(String.format("Invalid port number for the Sidecar service: %d",
                                                             port));
        }
        this.port = port;
        this.hostname = Objects.requireNonNull(hostname, "The Sidecar hostname must be non-null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int port()
    {
        return port;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String hostname()
    {
        return hostname;
    }

    /**
     * {@inheritDoc}
     */
    @Override

    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        SidecarInstanceImpl that = (SidecarInstanceImpl) o;
        return port == that.port && Objects.equals(hostname, that.hostname);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(port, hostname);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "SidecarInstanceImpl{" +
               "port=" + port +
               ", hostname='" + hostname + '\'' +
               '}';
    }
}

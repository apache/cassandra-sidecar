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

package org.apache.cassandra.sidecar.common.containers;

/**
 * A Cassandra container that adds functionality to the testcontainers
 * {@link org.testcontainers.containers.CassandraContainer} for JMX.
 */
public class CassandraContainer extends org.testcontainers.containers.CassandraContainer<CassandraContainer>
{
    public static final String RMI_SERVER_HOSTNAME = "127.0.0.1";
    public static final Integer JMX_PORT = 7199;

    /**
     * Constructs a new {@link CassandraContainer} with the {@link #JMX_PORT JMX port} exposed
     *
     * @param dockerImageName the name of the docker image to use for the container
     */
    public CassandraContainer(String dockerImageName)
    {
        super(dockerImageName);
        addFixedExposedPort(JMX_PORT, JMX_PORT);

        addEnv("LOCAL_JMX", "no");
        addEnv("JVM_EXTRA_OPTS", "-Dcom.sun.management.jmxremote=true"
                                 + " -Djava.rmi.server.hostname=" + RMI_SERVER_HOSTNAME
                                 + " -Dcom.sun.management.jmxremote.port=" + JMX_PORT
                                 + " -Dcom.sun.management.jmxremote.rmi.port=" + JMX_PORT
                                 + " -Dcom.sun.management.jmxremote.authenticate=false"
                                 + " -Dcom.sun.management.jmxremote.local.only=false"
                                 + " -Dcom.sun.management.jmxremote.ssl=false"
        );
    }
}

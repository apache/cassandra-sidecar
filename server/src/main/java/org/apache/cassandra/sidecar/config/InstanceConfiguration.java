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

package org.apache.cassandra.sidecar.config;

import java.util.List;

/**
 * Encapsulates the basic configuration needed to connect to a single Cassandra instance
 */
public interface InstanceConfiguration
{
    /**
     * @return an identifier for the Cassandra instance
     */
    int id();

    /**
     * @return the host address for the Cassandra instance
     */
    String host();

    /**
     * @return the port number for the Cassandra instance
     */
    int port();

    /**
     * @return the username used for connecting to the Cassandra instance
     */
    String username();

    /**
     * @return the password used for connecting to the Cassandra instance
     */
    String password();

    /**
     * @return a list of data directories of cassandra instance
     */
    List<String> dataDirs();

    /**
     * @return staging directory for the uploads of the cassandra instance
     */
    String stagingDir();

    /**
     * @return the host address of the JMX service for the Cassandra instance
     */
    String jmxHost();

    /**
     * @return the port number for the JMX service for the Cassandra instance
     */
    int jmxPort();

    /**
     * @return the port number of the Cassandra instance
     */
    boolean jmxSslEnabled();

    /**
     * @return the name of the JMX role for the JMX service for the Cassandra instance
     */
    String jmxRole();

    /**
     * @return the password for the JMX role for the JMX service for the Cassandra instance
     */
    String jmxRolePassword();
}

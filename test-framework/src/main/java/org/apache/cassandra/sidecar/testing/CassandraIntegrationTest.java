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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Interface to mark an integration test which should be run against multiple Cassandra versions
 */
@TestTemplate
@Target({ ElementType.ANNOTATION_TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Tag("integrationTest")
@ExtendWith(CassandraTestTemplate.class)
public @interface CassandraIntegrationTest
{
    /**
     * Returns the number of initial nodes per datacenter for the integration tests. Defaults to 1 node per datacenter.
     *
     * @return the number of nodes per datacenter for the integration tests
     * */
    int nodesPerDc() default 1;

    /**
     * Returns the number of nodes expected to be added by the end of the integration test. Defaults ot 0.
     * @return the number of nodes the test expects to add for the integration test.
     */
    int newNodesPerDc() default 0;

    /**
     * Returns the number of datacenters to configure for the integration test. Defaults to 1 datacenter.
     *
     * @return the number of datacenters to configure for the integration test
     */
    int numDcs() default 1;

    /**
     * Returns the number of data directories to use per instance. Cassandra supports multiple data directories
     * for each instance. Defaults to 1 data directory per instance.
     *
     * @return the number of data directories to use per instance
     */
    int numDataDirsPerInstance() default 1;

    /**
     * Returns whether gossip is enabled or disabled for the integration test. Defaults to {@code false}.
     *
     * @return whether gossip is enabled or disabled for the integration test
     */
    boolean gossip() default false;

    /**
     * Returns whether internode networking is enabled or disabled for the integration test. Defaults to {@code false}.
     *
     * @return whether internode networking is enabled or disabled for the integration test
     */
    boolean network() default false;

    /**
     * Returns whether JMX is enabled or disabled for the integration test. Defaults to {@code true}.
     *
     * @return whether JMX is enabled or disabled for the integration test
     */
    boolean jmx() default true;

    /**
     * Returns whether the native transport protocol is enabled or disabled for the integration test. Defaults to
     * {@code true}.
     *
     * @return whether the native transport protocol is enabled or disabled for the integration test
     */
    boolean nativeTransport() default true;

}

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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.apache.cassandra.sidecar.common.testing.CassandraIntegrationTest;
import org.apache.cassandra.sidecar.common.testing.CassandraTestContext;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Placeholder test
 */
class StatusTest
{
    private static final Logger logger = LoggerFactory.getLogger(StatusTest.class);

    @BeforeEach
    void setupData(CassandraTestContext context)
    {
        assertThat(context.container.isRunning()).isTrue();
        logger.info("Running Cassandra on host={}", context.container.getContactPoint());
    }

    @CassandraIntegrationTest
    @DisplayName("Ensure status returns correctly")
    void testSomething(CassandraTestContext context)
    {
        logger.info("test context in test {}", context);
        Session session = context.session.localCql();
        assertThat(session).isNotNull();
        assert session != null; // quiet spotbugs
        ResultSet rs = session.execute("SELECT * from system.peers_v2");
        assertThat(rs).isNotNull();
    }
}

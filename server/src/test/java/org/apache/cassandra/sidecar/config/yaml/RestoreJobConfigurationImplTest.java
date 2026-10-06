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

package org.apache.cassandra.sidecar.config.yaml;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.RestoreJobConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class RestoreJobConfigurationImplTest
{
    @Test
    void testYamlRejectsTtlLessThanMinimum()
    {
        String yaml = "blob_restore:\n" +
                      "  restore_job_tables_ttl: 13d";

        assertThatExceptionOfType(JsonMappingException.class)
        .isThrownBy(() -> SidecarConfigurationImpl.fromYamlString(yaml))
        .withRootCauseInstanceOf(IllegalArgumentException.class)
        .withMessageContaining("restoreJobTablesTtl cannot be less than");
    }

    @Test
    void testYamlRejectsRecencyDaysThatGreaterThanTtl()
    {
        // Validation depends on both values, so we need to set both
        String yaml = "blob_restore:\n" +
                      "  restore_job_tables_ttl: 15d\n" +
                      "  job_discovery_minimum_recency_days: 20";

        assertThatExceptionOfType(JsonMappingException.class)
        .isThrownBy(() -> SidecarConfigurationImpl.fromYamlString(yaml))
        .withRootCauseInstanceOf(IllegalArgumentException.class)
        .withMessageContaining("JobDiscoveryMinimumRecencyDays (in seconds) cannot be greater than");
    }

    @Test
    void testYamlAcceptsRecencyDaysLessThanTtl() throws IOException
    {
        String yaml = "blob_restore:\n" +
                      "  restore_job_tables_ttl: 21d\n" +
                      "  job_discovery_minimum_recency_days: 20";

        RestoreJobConfiguration config = SidecarConfigurationImpl.fromYamlString(yaml).restoreJobConfiguration();

        assertThat(config.restoreJobTablesTtl()).isEqualTo(SecondBoundConfiguration.parse("21d"));
        assertThat(config.jobDiscoveryMinimumRecencyDays()).isEqualTo(20);
    }
}

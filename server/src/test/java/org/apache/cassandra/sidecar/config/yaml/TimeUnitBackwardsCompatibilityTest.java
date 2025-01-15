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
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.CdcConfiguration;
import org.apache.cassandra.sidecar.config.CoordinationConfiguration;
import org.apache.cassandra.sidecar.config.JmxConfiguration;
import org.apache.cassandra.sidecar.config.KeyStoreConfiguration;
import org.apache.cassandra.sidecar.config.PeriodicTaskConfiguration;
import org.apache.cassandra.sidecar.config.RestoreJobConfiguration;
import org.apache.cassandra.sidecar.config.S3ClientConfiguration;
import org.apache.cassandra.sidecar.config.SSTableImportConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.config.ThrottleConfiguration;
import org.apache.cassandra.sidecar.config.TrafficShapingConfiguration;
import org.apache.cassandra.sidecar.config.WorkerPoolConfiguration;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Tests for {@link DurationSpec} based configurations
 */
class TimeUnitBackwardsCompatibilityTest
{
    @DisplayName("Configuration for a duration spec type")
    @Test
    void testDurationSpec() throws Exception
    {
        String yaml = "sidecar:\n" +
                      "  coordination:\n" +
                      "    cluster_lease_claim:\n" +
                      "      enabled: true\n" +
                      "      initial_delay: 19s\n" +
                      "      execute_interval: 5m";
        SidecarConfiguration config = SidecarConfigurationImpl.fromYamlString(yaml);

        ServiceConfiguration serviceConfig = config.serviceConfiguration();
        assertThat(serviceConfig).isNotNull();
        CoordinationConfiguration coordination = serviceConfig.coordinationConfiguration();
        assertThat(coordination).isNotNull();
        PeriodicTaskConfiguration periodicTask = coordination.clusterLeaseClaimConfiguration();
        assertThat(periodicTask).isNotNull();
        assertThat(periodicTask.initialDelay().unit()).isEqualTo(TimeUnit.SECONDS);
        assertThat(periodicTask.initialDelay().quantity()).isEqualTo(19L);
        assertThat(periodicTask.initialDelay().toMillis()).isEqualTo(19_000L);
        assertThat(periodicTask.executeInterval().unit()).isEqualTo(TimeUnit.MINUTES);
        assertThat(periodicTask.executeInterval().quantity()).isEqualTo(5L);
        assertThat(periodicTask.executeInterval().toMillis()).isEqualTo(300_000L);
    }

    // PeriodicTaskConfiguration

    @DisplayName("Last configured value takes precedence between initial_delay and initial_delay_millis")
    @Test
    void testConflictingInitialDelayConfiguration() throws IOException
    {
        String yaml = "sidecar:\n" +
                      "  coordination:\n" +
                      "    cluster_lease_claim:\n" +
                      "      initial_delay: 19s\n" +
                      "      initial_delay_millis: 19_000";


        SidecarConfigurationImpl sidecarConfiguration = SidecarConfigurationImpl.fromYamlString(yaml);
        PeriodicTaskConfiguration config = sidecarConfiguration.serviceConfiguration
                                           .coordinationConfiguration()
                                           .clusterLeaseClaimConfiguration();
        assertThat(config).isNotNull();
        assertThat(config.initialDelay().toMillis()).isEqualTo(19_000L);
    }

    @DisplayName("Last configured value takes precedence between poll_freq_millis and execute_interval_millis")
    @Test
    void testConflictingExecuteIntervalConfiguration() throws IOException
    {
        String yaml = "healthcheck:\n" +
                      "  initial_delay_millis: 245\n" +
                      "  poll_freq_millis: 65000\n" +
                      "  execute_interval_millis: 75000";

        SidecarConfigurationImpl sidecarConfiguration = SidecarConfigurationImpl.fromYamlString(yaml);
        PeriodicTaskConfiguration config = sidecarConfiguration.healthCheckConfiguration;
        assertThat(config).isNotNull();
        assertThat(config.executeInterval().toMillis()).isEqualTo(75_000);
    }

    @DisplayName("Supports legacy name 'poll_freq_millis' for the 'healthcheck' configuration")
    @Test
    void testLegacyHealthCheck() throws IOException
    {
        String yaml = "healthcheck:\n" +
                      "  initial_delay_millis: 245\n" +
                      "  poll_freq_millis: 65000";

        SidecarConfigurationImpl config = SidecarConfigurationImpl.fromYamlString(yaml);
        PeriodicTaskConfiguration healthCheckConfig = config.healthCheckConfiguration();
        assertThat(healthCheckConfig).isNotNull();
        assertThat(healthCheckConfig.initialDelay().quantity()).isEqualTo(245);
        assertThat(healthCheckConfig.initialDelay().unit()).isEqualTo(TimeUnit.MILLISECONDS);
        assertThat(healthCheckConfig.initialDelay().toMillis()).isEqualTo(245);
        assertThat(healthCheckConfig.executeInterval().quantity()).isEqualTo(65_000L);
        assertThat(healthCheckConfig.executeInterval().unit()).isEqualTo(TimeUnit.MILLISECONDS);
        assertThat(healthCheckConfig.executeInterval().toMillis()).isEqualTo(65_000L);
    }

    // ServiceConfiguration

    @DisplayName("Supports legacy name 'request_idle_timeout_millis' for the 'sidecar' configuration")
    @Test
    void testLegacyRequestIdleTimeoutConfiguration() throws IOException
    {
        String yaml = "sidecar:\n" +
                      "  request_idle_timeout_millis: 21000";

        SidecarConfigurationImpl config = SidecarConfigurationImpl.fromYamlString(yaml);
        ServiceConfiguration serviceConfiguration = config.serviceConfiguration();
        assertThat(serviceConfiguration).isNotNull();
        assertThat(serviceConfiguration.requestIdleTimeout().toMillis()).isEqualTo(21_000);
    }

    @DisplayName("Supports legacy name 'request_idle_timeout_millis' for the 'sidecar' configuration")
    @Test
    void testLegacyRequestTimeoutConfiguration() throws IOException
    {
        String yaml = "sidecar:\n" +
                      "  request_timeout_millis: 11100";

        SidecarConfigurationImpl config = SidecarConfigurationImpl.fromYamlString(yaml);
        ServiceConfiguration serviceConfiguration = config.serviceConfiguration();
        assertThat(serviceConfiguration).isNotNull();
        assertThat(serviceConfiguration.requestTimeout().toMillis()).isEqualTo(11_100);
    }

    @DisplayName("Supports legacy name 'allowable_time_skew_in_minutes' for the 'sidecar' configuration")
    @Test
    void testLegacyAllowableTimeSkewConfiguration() throws IOException
    {
        String yaml = "sidecar:\n" +
                      "  allowable_time_skew_in_minutes: 37";

        SidecarConfigurationImpl config = SidecarConfigurationImpl.fromYamlString(yaml);
        ServiceConfiguration serviceConfiguration = config.serviceConfiguration();
        assertThat(serviceConfiguration).isNotNull();
        assertThat(serviceConfiguration.allowableTimeSkew().quantity()).isEqualTo(37);
        assertThat(serviceConfiguration.allowableTimeSkew().unit()).isEqualTo(TimeUnit.MINUTES);
    }

    @DisplayName("Fails when the 'allowable_time_skew' configuration is less than the minimum allowable value")
    @Test
    void failsOnInvalidAllowableTimeSkewConfiguration()
    {
        String yaml = "sidecar:\n" +
                      "  allowable_time_skew: 0m";

        assertThatExceptionOfType(JsonMappingException.class)
        .isThrownBy(() -> SidecarConfigurationImpl.fromYamlString(yaml))
        .withRootCauseInstanceOf(ConfigurationException.class)
        .withMessageContaining("Invalid allowable_time_skew value (0m). The minimum allowed value is 1 minute (1m)");
    }

    // ThrottleConfiguration

    @DisplayName("Supports legacy name 'timeout_sec' for the 'sidecar.throttle' configuration")
    @Test
    void testLegacyTimeoutConfiguration() throws IOException
    {
        String yaml = "sidecar:\n" +
                      "  throttle:\n" +
                      "    timeout_sec: 25";

        SidecarConfigurationImpl config = SidecarConfigurationImpl.fromYamlString(yaml);
        ThrottleConfiguration throttleConfig = config.serviceConfiguration().throttleConfiguration();
        assertThat(throttleConfig).isNotNull();
        assertThat(throttleConfig.timeout().toSeconds()).isEqualTo(25);
    }

    // SSTableImportConfiguration

    @DisplayName("Supports legacy name 'poll_interval_millis' for the 'sidecar.sstable_import' configuration")
    @Test
    void testLegacySSTableImportPollIntervalConfiguration() throws IOException
    {
        String yaml = "sidecar:\n" +
                      "  sstable_import:\n" +
                      "    poll_interval_millis: 275";

        SidecarConfigurationImpl config = SidecarConfigurationImpl.fromYamlString(yaml);
        SSTableImportConfiguration importConfig = config.serviceConfiguration().sstableImportConfiguration();
        assertThat(importConfig).isNotNull();
        assertThat(importConfig.executeInterval().quantity()).isEqualTo(275);
        assertThat(importConfig.executeInterval().unit()).isEqualTo(TimeUnit.MILLISECONDS);
        assertThat(importConfig.initialDelay().quantity()).isEqualTo(275);
        assertThat(importConfig.initialDelay().unit()).isEqualTo(TimeUnit.MILLISECONDS);
    }

    // TrafficShapingConfiguration

    @DisplayName("Supports legacy names 'max_delay_to_wait_millis' and 'check_interval_for_stats_millis' for the 'sidecar.traffic_shaping' configuration")
    @Test
    void testLegacyTrafficShapingConfiguration() throws IOException
    {
        String yaml = "sidecar:\n" +
                      "  traffic_shaping:\n" +
                      "    max_delay_to_wait_millis: 12000\n" +
                      "    check_interval_for_stats_millis: 82000";

        SidecarConfigurationImpl config = SidecarConfigurationImpl.fromYamlString(yaml);
        TrafficShapingConfiguration trafficShapingConfig = config.serviceConfiguration.trafficShapingConfiguration();
        assertThat(trafficShapingConfig).isNotNull();
        assertThat(trafficShapingConfig.maxDelayToWait().toMillis()).isEqualTo(12_000L);
        assertThat(trafficShapingConfig.checkIntervalForStats().toMillis()).isEqualTo(82_000L);
    }

    // CacheConfiguration

    @DisplayName("Supports legacy name 'expire_after_access_millis' for the 'sidecar.sstable_import.cache' configuration")
    @Test
    void testLegacyExpireAfterAccessConfiguration() throws IOException
    {
        String yaml = "sidecar:\n" +
                      "  sstable_import:\n" +
                      "    cache:\n" +
                      "      expire_after_access_millis: 32000";

        SidecarConfigurationImpl config = SidecarConfigurationImpl.fromYamlString(yaml);
        CacheConfiguration cacheConfig = config.serviceConfiguration().sstableImportConfiguration().cacheConfiguration();
        assertThat(cacheConfig).isNotNull();
        assertThat(cacheConfig.expireAfterAccess().toMillis()).isEqualTo(32_000L);
    }

    @DisplayName("Supports legacy name 'warmup_retry_interval_millis' for the 'access_control.permission_cache' configuration")
    @Test
    void testLegacyWarmupRetryIntervalConfiguration() throws IOException
    {
        String yaml = "access_control:\n" +
                      "  permission_cache:\n" +
                      "    warmup_retry_interval_millis: 60000";

        SidecarConfigurationImpl config = SidecarConfigurationImpl.fromYamlString(yaml);
        CacheConfiguration cacheConfig = config.accessControlConfiguration().permissionCacheConfiguration();
        assertThat(cacheConfig).isNotNull();
        assertThat(cacheConfig.warmupRetryInterval().toMillis()).isEqualTo(60_000L);
    }

    // WorkerPoolConfiguration

    @DisplayName("Supports legacy name 'max_execution_time_millis' for the 'sidecar.worker_pools.service' and 'sidecar.worker_pools.internal' configurations")
    @Test
    void testLegacyWorkerMaxExecutionTimeConfiguration() throws IOException
    {
        String yaml = "sidecar:\n" +
                      "  worker_pools:\n" +
                      "    service:\n" +
                      "      max_execution_time_millis: 20000\n" +
                      "    internal:\n" +
                      "      max_execution_time_millis: 33100";

        SidecarConfigurationImpl config = SidecarConfigurationImpl.fromYamlString(yaml);
        WorkerPoolConfiguration service = config.serviceConfiguration.serverWorkerPoolConfiguration();
        assertThat(service.workerMaxExecutionTime().toMillis()).isEqualTo(20_000);
        WorkerPoolConfiguration internal = config.serviceConfiguration.serverInternalWorkerPoolConfiguration();
        assertThat(internal.workerMaxExecutionTime().toMillis()).isEqualTo(33_100);
    }

    // JmxConfiguration

    @DisplayName("Supports legacy name 'retry_delay_millis' for the 'sidecar.jmx' configuration")
    @Test
    void testLegacyJmxRetryDelayConfiguration() throws IOException
    {
        String yaml = "sidecar:\n" +
                      "  jmx:\n" +
                      "    retry_delay_millis: 525";

        SidecarConfigurationImpl config = SidecarConfigurationImpl.fromYamlString(yaml);
        JmxConfiguration jmxConfig = config.serviceConfiguration().jmxConfiguration();
        assertThat(jmxConfig).isNotNull();
        assertThat(jmxConfig.retryDelay().toMillis()).isEqualTo(525);
    }

    // CdcConfiguration

    @DisplayName("Supports legacy name 'segment_hardlink_cache_expiry_in_secs' for the 'sidecar.cdc' configuration")
    @Test
    void testLegacyCdcSegmentHardLinkCacheExpiryConfiguration() throws IOException
    {
        String yaml = "sidecar:\n" +
                      "  cdc:\n" +
                      "    segment_hardlink_cache_expiry_in_secs: 25";

        SidecarConfigurationImpl config = SidecarConfigurationImpl.fromYamlString(yaml);
        CdcConfiguration cdcConfig = config.serviceConfiguration().cdcConfiguration();
        assertThat(cdcConfig).isNotNull();
        assertThat(cdcConfig.segmentHardLinkCacheExpiry().toSeconds()).isEqualTo(25);
    }

    // KeyStoreConfiguration

    @DisplayName("Supports legacy name 'check_interval_sec' for the 'ssl.keystore' configuration")
    @Test
    void testLegacyKeyStoreCheckIntervalConfiguration() throws IOException
    {
        String yaml = "ssl:\n" +
                      "  keystore:\n" +
                      "    check_interval_sec: 23";

        SidecarConfigurationImpl config = SidecarConfigurationImpl.fromYamlString(yaml);
        KeyStoreConfiguration keystoreConfig = config.sslConfiguration().keystore();
        assertThat(keystoreConfig).isNotNull();
        assertThat(keystoreConfig.checkInterval().toSeconds()).isEqualTo(23);
    }

    // SslConfiguration

    @DisplayName("Supports legacy name 'handshake_timeout_sec' for the 'ssl' configuration")
    @Test
    void testLegacySslHandshakeTimeoutConfiguration() throws IOException
    {
        String yaml = "ssl:\n" +
                      "  handshake_timeout_sec: 25";

        SidecarConfigurationImpl config = SidecarConfigurationImpl.fromYamlString(yaml);
        SslConfiguration sslConfig = config.sslConfiguration();
        assertThat(sslConfig).isNotNull();
        assertThat(sslConfig.handshakeTimeout().toSeconds()).isEqualTo(25);
    }

    // S3ClientConfiguration

    @DisplayName("Supports legacy name 'thread_keep_alive_seconds' for the 's3_client' configuration")
    @Test
    void testLegacyS3ClientThreadKeepAliveConfiguration() throws IOException
    {
        String yaml = "s3_client:\n" +
                      "  thread_keep_alive_seconds: 152";

        SidecarConfigurationImpl config = SidecarConfigurationImpl.fromYamlString(yaml);
        S3ClientConfiguration s3ClientConfig = config.s3ClientConfiguration();
        assertThat(s3ClientConfig).isNotNull();
        assertThat(s3ClientConfig.threadKeepAlive().toSeconds()).isEqualTo(152);
    }

    @DisplayName("Supports legacy name 'api_call_timeout_millis' for the 's3_client' configuration")
    @Test
    void testLegacyS3ClientApiCallTimeoutConfiguration() throws IOException
    {
        String yaml = "s3_client:\n" +
                      "  api_call_timeout_millis: 777";

        SidecarConfigurationImpl config = SidecarConfigurationImpl.fromYamlString(yaml);
        S3ClientConfiguration s3ClientConfig = config.s3ClientConfiguration();
        assertThat(s3ClientConfig).isNotNull();
        assertThat(s3ClientConfig.apiCallTimeout().toMillis()).isEqualTo(777);
    }

    // RestoreJobConfiguration

    @DisplayName("Supports legacy name 'job_discovery_active_loop_delay_millis' for the 'blob_restore' configuration")
    @Test
    void testLegacyRestoreJobDiscoveryActiveLoopDelayConfiguration() throws IOException
    {
        String yaml = "blob_restore:\n" +
                      "  job_discovery_active_loop_delay_millis: 1234567";

        SidecarConfigurationImpl config = SidecarConfigurationImpl.fromYamlString(yaml);
        RestoreJobConfiguration restoreJobConfig = config.restoreJobConfiguration();
        assertThat(restoreJobConfig).isNotNull();
        assertThat(restoreJobConfig.jobDiscoveryActiveLoopDelay().toMillis()).isEqualTo(1234567);
    }

    @DisplayName("Supports legacy name 'job_discovery_idle_loop_delay_millis' for the 'blob_restore' configuration")
    @Test
    void testLegacyRestoreJobDiscoveryIdleLoopDelayConfiguration() throws IOException
    {
        String yaml = "blob_restore:\n" +
                      "  job_discovery_idle_loop_delay_millis: 76543";

        SidecarConfigurationImpl config = SidecarConfigurationImpl.fromYamlString(yaml);
        RestoreJobConfiguration restoreJobConfig = config.restoreJobConfiguration();
        assertThat(restoreJobConfig).isNotNull();
        assertThat(restoreJobConfig.jobDiscoveryIdleLoopDelay().toMillis()).isEqualTo(76543);
    }

    @DisplayName("Supports legacy name 'restore_job_tables_ttl_seconds' for the 'blob_restore' configuration")
    @Test
    void testLegacyRestoreJobTablesTtlConfiguration() throws IOException
    {
        String yaml = "blob_restore:\n" +
                      "  restore_job_tables_ttl_seconds: 76543";

        SidecarConfigurationImpl config = SidecarConfigurationImpl.fromYamlString(yaml);
        RestoreJobConfiguration restoreJobConfig = config.restoreJobConfiguration();
        assertThat(restoreJobConfig).isNotNull();
        assertThat(restoreJobConfig.restoreJobTablesTtl().toSeconds()).isEqualTo(76543);
    }

    @DisplayName("Supports legacy name 'ring_topology_refresh_delay_millis' for the 'blob_restore' configuration")
    @Test
    void testLegacyRestoreJobRingTopologyRefreshDelayConfiguration() throws IOException
    {
        String yaml = "blob_restore:\n" +
                      "  ring_topology_refresh_delay_millis: 724831";

        SidecarConfigurationImpl config = SidecarConfigurationImpl.fromYamlString(yaml);
        RestoreJobConfiguration restoreJobConfig = config.restoreJobConfiguration();
        assertThat(restoreJobConfig).isNotNull();
        assertThat(restoreJobConfig.ringTopologyRefreshDelay().toMillis()).isEqualTo(724831);
    }

    @DisplayName("Supports legacy name 'slow_task_threshold_seconds' for the 'blob_restore' configuration")
    @Test
    void testLegacyRestoreJobSlowTaskThresholdConfiguration() throws IOException
    {
        String yaml = "blob_restore:\n" +
                      "  slow_task_threshold_seconds: 27";

        SidecarConfigurationImpl config = SidecarConfigurationImpl.fromYamlString(yaml);
        RestoreJobConfiguration restoreJobConfig = config.restoreJobConfiguration();
        assertThat(restoreJobConfig).isNotNull();
        assertThat(restoreJobConfig.slowTaskThreshold().toSeconds()).isEqualTo(27);
    }

    @DisplayName("Supports legacy name 'slow_task_report_delay_seconds' for the 'blob_restore' configuration")
    @Test
    void testLegacyRestoreJobSlowTaskReportDelayConfiguration() throws IOException
    {
        String yaml = "blob_restore:\n" +
                      "  slow_task_report_delay_seconds: 35";

        SidecarConfigurationImpl config = SidecarConfigurationImpl.fromYamlString(yaml);
        RestoreJobConfiguration restoreJobConfig = config.restoreJobConfiguration();
        assertThat(restoreJobConfig).isNotNull();
        assertThat(restoreJobConfig.slowTaskReportDelay().toSeconds()).isEqualTo(35);
    }
}

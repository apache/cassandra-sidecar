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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.yaml.MetricsFilteringConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.assertj.core.api.Condition;

import static org.apache.cassandra.sidecar.common.ResourceUtils.writeResourceToPath;
import static org.apache.cassandra.sidecar.config.yaml.MetricsFilteringConfigurationImpl.EQUALS_TYPE;
import static org.apache.cassandra.sidecar.config.yaml.MetricsFilteringConfigurationImpl.REGEX_TYPE;
import static org.apache.cassandra.sidecar.config.yaml.VertxMetricsConfigurationImpl.DEFAULT_JMX_DOMAIN_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

/**
 * Tests reading Sidecar {@link SidecarConfiguration} from YAML files
 */
class SidecarConfigurationTest
{
    @TempDir
    private Path configPath;

    @Test
    void testSidecarConfiguration() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_multiple_instances.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);
        validateMultipleInstancesSidecarConfiguration(config, false);
    }

    @Test
    void testLegacySidecarYAMLFormatWithSingleInstance() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_single_instance.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);
        validateSingleInstanceSidecarConfiguration(config);
    }

    @Test
    void testReadAllowableTimeSkew() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_custom_allowable_time_skew.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);
        assertThat(config.serviceConfiguration()).isNotNull();
        assertThat(config.serviceConfiguration().allowableTimeSkew().quantity()).isEqualTo(1);
        assertThat(config.serviceConfiguration().allowableTimeSkew().unit()).isEqualTo(TimeUnit.MINUTES);
    }

    @Test
    void testReadingSingleInstanceSectionOverMultipleInstances() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_with_single_multiple_instances.yaml");
        SidecarConfiguration configuration = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);
        assertThat(configuration.cassandraInstances()).isNotNull().hasSize(1);

        InstanceConfiguration i1 = configuration.cassandraInstances().get(0);
        assertThat(i1.host()).isEqualTo("localhost");
        assertThat(i1.port()).isEqualTo(9042);
    }

    @Test
    void testReadingCassandraInputValidation() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_validation_configuration.yaml");
        SidecarConfiguration configuration = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);
        CassandraInputValidationConfiguration validationConfiguration =
        configuration.cassandraInputValidationConfiguration();

        assertThat(validationConfiguration.validatorConfiguration()).isNotNull();
        assertThat(validationConfiguration.validatorConfiguration().className())
        .isEqualTo("org.apache.cassandra.sidecar.utils.FastCassandraInputValidator");
        assertThat(validationConfiguration.validatorConfiguration().namedParameters())
        .containsExactlyInAnyOrderEntriesOf(Map.of("valid_terminations", ".abc,.def",
                                                   "valid_restricted_terminations", ".xml"));
        assertThat(validationConfiguration.forbiddenKeyspaces()).contains("a", "b", "c");
        assertThat(validationConfiguration.allowedPatternForName()).isEqualTo("[a-z]+");
        assertThat(validationConfiguration.allowedPatternForQuotedName()).isEqualTo("[A-Z]+");
        assertThat(validationConfiguration.allowedPatternForComponentName())
        .isEqualTo("(\\.db|\\.cql|\\.json|\\.crc32|TOC\\.txt)");
        assertThat(validationConfiguration.allowedPatternForRestrictedComponentName())
        .isEqualTo("(\\.db|TOC\\.txt)");
    }

    @Test
    void testReadingJmxConfiguration() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_multiple_instances.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);
        assertThat(config.serviceConfiguration().jmxConfiguration()).isNotNull();
        JmxConfiguration jmxConfiguration = config.serviceConfiguration().jmxConfiguration();
        assertThat(jmxConfiguration.maxRetries()).isEqualTo(1);
        assertThat(jmxConfiguration.retryDelay().quantity()).isEqualTo(1234L);
        assertThat(jmxConfiguration.retryDelay().unit()).isEqualTo(TimeUnit.MILLISECONDS);
    }

    @Test
    void testReadingBlankJmxConfigurationReturnsDefaults() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_missing_jmx.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);
        assertThat(config.serviceConfiguration().jmxConfiguration()).isNotNull();
        JmxConfiguration jmxConfiguration = config.serviceConfiguration().jmxConfiguration();
        assertThat(jmxConfiguration.maxRetries()).isEqualTo(3);
        assertThat(jmxConfiguration.retryDelay().quantity()).isEqualTo(200L);
        assertThat(jmxConfiguration.retryDelay().unit()).isEqualTo(TimeUnit.MILLISECONDS);
    }

    @Test
    void testUploadsConfiguration() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_multiple_instances.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);

        assertThat(config.serviceConfiguration()).isNotNull();
        SSTableUploadConfiguration uploadConfiguration = config.serviceConfiguration()
                                                               .sstableUploadConfiguration();
        assertThat(uploadConfiguration).isNotNull();

        assertThat(uploadConfiguration.concurrentUploadsLimit()).isEqualTo(80);
        assertThat(uploadConfiguration.minimumSpacePercentageRequired()).isEqualTo(10);
    }

    @Test
    void testSslConfiguration() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_ssl.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);
        validateMultipleInstancesSidecarConfiguration(config, true);
    }

    @Test
    void testFilePermissions() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_file_permissions.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);

        assertThat(config).isNotNull();
        assertThat(config.serviceConfiguration()).isNotNull();
        assertThat(config.serviceConfiguration().sstableUploadConfiguration()).isNotNull();
        assertThat(config.serviceConfiguration().sstableUploadConfiguration().filePermissions()).isEqualTo("rw-rw-rw-");
    }

    @Test
    void testInvalidFilePermissions()
    {
        Path yamlPath = yaml("config/sidecar_invalid_file_permissions.yaml");
        assertThatExceptionOfType(JsonMappingException.class)
        .isThrownBy(() -> SidecarConfigurationImpl.readYamlConfiguration(yamlPath))
        .withRootCauseInstanceOf(IllegalArgumentException.class)
        .withMessageContaining("Invalid file_permissions configuration=\"not-valid\"");
    }

    @Test
    void testInvalidClientAuth()
    {
        Path yamlPath = yaml("config/sidecar_invalid_client_auth.yaml");
        assertThatExceptionOfType(JsonMappingException.class)
        .isThrownBy(() -> SidecarConfigurationImpl.readYamlConfiguration(yamlPath))
        .withRootCauseInstanceOf(IllegalArgumentException.class)
        .withMessageContaining("Invalid client_auth configuration=\"notvalid\", " +
                               "valid values are (NONE,REQUEST,REQUIRED)");
    }

    @Test
    void testDriverParameters() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_driver_params.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);

        DriverConfiguration driverConfiguration = config.driverConfiguration();
        assertThat(driverConfiguration).isNotNull();
        assertThat(driverConfiguration.localDc()).isEqualTo("dc1");
        List<InetSocketAddress> endpoints = Arrays.asList(new InetSocketAddress("127.0.0.1", 9042),
                                                          new InetSocketAddress("127.0.0.2", 9042));
        assertThat(driverConfiguration.contactPoints()).isEqualTo(endpoints);
        assertThat(driverConfiguration.numConnections()).isEqualTo(6);
        assertThat(driverConfiguration.username()).isEqualTo("cassandra");
        assertThat(driverConfiguration.password()).isEqualTo("cassandra");
        SslConfiguration sslConfiguration = driverConfiguration.sslConfiguration();
        assertThat(sslConfiguration.enabled()).isTrue();
        assertThat(sslConfiguration.secureTransportProtocols().size()).isEqualTo(2);
        assertThat(sslConfiguration.isKeystoreConfigured()).isTrue();
        assertThat(sslConfiguration.keystore().type()).isEqualTo("PKCS12");
        assertThat(sslConfiguration.keystore().path()).isEqualTo("path/to/keystore.p12");
        assertThat(sslConfiguration.keystore().password()).isEqualTo("password");
        assertThat(sslConfiguration.isTrustStoreConfigured()).isTrue();
        assertThat(sslConfiguration.truststore().type()).isEqualTo("PKCS12");
        assertThat(sslConfiguration.truststore().path()).isEqualTo("path/to/keystore.p12");
        assertThat(sslConfiguration.truststore().password()).isEqualTo("password");
    }

    @Test
    void testReadCustomSchemaKeyspaceConfiguration() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_schema_keyspace_configuration.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);

        SchemaKeyspaceConfiguration configuration = config.serviceConfiguration().schemaKeyspaceConfiguration();
        assertThat(configuration).isNotNull();
        assertThat(configuration.isEnabled()).isTrue();
        assertThat(configuration.keyspace()).isEqualTo("sidecar_internal");
        assertThat(configuration.replicationStrategy()).isEqualTo("SimpleStrategy");
        assertThat(configuration.replicationFactor()).isEqualTo(3);
        assertThat(configuration.createReplicationStrategyString())
        .isEqualTo("{'class':'SimpleStrategy', 'replication_factor':'3'}");
        assertThat(configuration.leaseSchemaTTL().toSeconds()).isEqualTo(300L);
    }

    @Test
    void testMissingSSTableSnapshotSection() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_missing_sstable_snapshot.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);

        assertThat(config.serviceConfiguration()).isNotNull();
        assertThat(config.serviceConfiguration().sstableSnapshotConfiguration()).isNull();
    }

    @Test
    void testMetricOptionsParsedFromYaml() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_metrics.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);

        MetricsConfiguration configuration = config.metricsConfiguration();
        assertThat(configuration.registryName()).isEqualTo("cassandra_sidecar_metrics");
        VertxMetricsConfiguration vertxMetricsConfiguration = configuration.vertxConfiguration();
        assertThat(vertxMetricsConfiguration.enabled()).isTrue();
        assertThat(vertxMetricsConfiguration.exposeViaJMX()).isFalse();
        assertThat(vertxMetricsConfiguration.jmxDomainName()).isEqualTo(DEFAULT_JMX_DOMAIN_NAME);
        List<MetricsFilteringConfiguration> includeConfigurations = configuration.includeConfigurations();
        List<MetricsFilteringConfiguration> excludeConfigurations = configuration.excludeConfigurations();
        assertThat(includeConfigurations.size()).isEqualTo(1);
        assertThat(excludeConfigurations.size()).isEqualTo(2);
        assertThat(includeConfigurations.get(0).type()).isEqualTo("regex");
        assertThat(includeConfigurations.get(0).value()).isEqualTo(".*");
        if (excludeConfigurations.get(0).type().equals("regex"))
        {
            assertThat(excludeConfigurations.get(0).value()).isEqualTo("vertx.eventbus.*");
            assertThat(excludeConfigurations.get(1).type()).isEqualTo("equals");
            assertThat(excludeConfigurations.get(1).value()).isEqualTo("instances_up");
        }
        else
        {
            assertThat(excludeConfigurations.get(1).value()).isEqualTo("vertx.eventbus.*");
            assertThat(excludeConfigurations.get(1).type()).isEqualTo("regex");
            assertThat(excludeConfigurations.get(0).value()).isEqualTo("instances_up");
        }
    }

    @Test
    void testInvalidMetricOptions()
    {
        Path yamlPath = yaml("config/sidecar_invalid_metrics.yaml");
        assertThatExceptionOfType(JsonMappingException.class)
        .isThrownBy(() -> SidecarConfigurationImpl.readYamlConfiguration(yamlPath))
        .withRootCauseInstanceOf(IllegalArgumentException.class)
        .withMessageContaining("contains passed for metric filtering is not recognized. Expected types are "
                               + REGEX_TYPE + " or " + EQUALS_TYPE);
    }

    @Test
    void testMetricsAllowedWithDefaultRegexFilter()
    {
        Pattern pattern = Pattern.compile(MetricsFilteringConfigurationImpl.DEFAULT_VALUE);
        assertThat(pattern.matcher("vertx.http.servers.localhost:0.responses-5xx")).matches();
        assertThat(pattern.matcher("sidecar.schema.cassandra_instances_up")).matches();
        assertThat(pattern.matcher("vertx.eventbus.messages.bytes-read")).matches();
        assertThat(pattern.matcher("throttled_429")).matches();
    }

    @Test
    void testSidecarPeerHealthConfiguration() throws IOException
    {
        String yaml = "sidecar_peer_health:\n" +
                      "  enabled: true\n" +
                      "  execute_interval: 63s\n" +
                      "  max_retries: 15\n" +
                      "  retry_delay: 5s";
        SidecarConfigurationImpl sidecarConfiguration = SidecarConfigurationImpl.fromYamlString(yaml);
        assertThat(sidecarConfiguration).isNotNull();
        SidecarPeerHealthConfiguration config = sidecarConfiguration.sidecarPeerHealthConfiguration();
        assertThat(config).isNotNull();
        assertThat(config.enabled()).isTrue();
        assertThat(config.executeInterval().toMillis()).isEqualTo(63_000);
        assertThat(config.maxRetries()).isEqualTo(15);
        assertThat(config.retryDelay().toMillis()).isEqualTo(5_000);
    }

    @Test
    void testSidecarClientConfiguration() throws IOException
    {
        String yaml = "sidecar_client:\n" +
                      "  request_timeout: 1s\n" +
                      "  request_idle_timeout: 1s\n" +
                      "  connection_pool_max_size: 10\n" +
                      "  connection_pool_clearing_period: 10s\n" +
                      "  connection_pool_event_loop_size: 10\n" +
                      "  connection_pool_max_wait_queue_size: 10\n" +
                      "  max_retries: 3\n" +
                      "  retry_delay: 1s\n" +
                      "  max_retry_delay: 2s\n" +
                      "  ssl:\n" +
                      "    enabled: false\n" +
                      "    keystore:\n" +
                      "      type: PKCS12\n" +
                      "      path: path/to/keystore.p12\n" +
                      "      password: password\n" +
                      "    truststore:\n" +
                      "      type: PKCS12\n" +
                      "      path: path/to/keystore.p12\n" +
                      "      password: password";
        SidecarConfigurationImpl sidecarConfiguration = SidecarConfigurationImpl.fromYamlString(yaml);
        assertThat(sidecarConfiguration).isNotNull();
        SidecarClientConfiguration config = sidecarConfiguration.sidecarClientConfiguration();
        assertThat(config).isNotNull();
        assertThat(config.sslConfiguration()).isNotNull();
        assertThat(config.sslConfiguration().enabled()).isFalse();
        assertThat(config.sslConfiguration().isKeystoreConfigured()).isTrue();
        assertThat(config.sslConfiguration().isTrustStoreConfigured()).isTrue();
        assertThat(config.requestTimeout().toMillis()).isEqualTo(1_000);
        assertThat(config.requestIdleTimeout().toMillis()).isEqualTo(1_000);
        assertThat(config.connectionPoolMaxSize()).isEqualTo(10);
        assertThat(config.connectionPoolCleanerPeriod().toMillis()).isEqualTo(10_000);
        assertThat(config.connectionPoolEventLoopSize()).isEqualTo(10);
        assertThat(config.connectionPoolMaxWaitQueueSize()).isEqualTo(10);
        assertThat(config.maxRetries()).isEqualTo(3);
        assertThat(config.retryDelay().toMillis()).isEqualTo(1_000);
        assertThat(config.maxRetryDelay().toMillis()).isEqualTo(2_000);
    }

    @Test
    void testVertxFilesystemOptionsConfiguration() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_vertx_filesystem_options.yaml");
        SidecarConfigurationImpl sidecarConfiguration = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);
        assertThat(sidecarConfiguration).isNotNull();
        assertThat(sidecarConfiguration.serviceConfiguration()).isNotNull();
        VertxConfiguration vertxConfiguration = sidecarConfiguration.vertxConfiguration();
        assertThat(vertxConfiguration).isNotNull();
        FileSystemOptionsConfiguration vertxFsOptions = vertxConfiguration.filesystemOptionsConfiguration();
        assertThat(vertxFsOptions).isNotNull();
        assertThat(vertxFsOptions.fileCachingEnabled()).isTrue();
        assertThat(vertxFsOptions.fileCacheDir()).isEqualTo("/path/to/vertx/cache");
        assertThat(vertxFsOptions.classpathResolvingEnabled()).isTrue();
    }

    @Test
    void testCdcConfiguration() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_cdc.yaml");
        SidecarConfigurationImpl sidecarConfiguration = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);
        assertThat(sidecarConfiguration).isNotNull();
        CdcConfiguration cdcConfig = sidecarConfiguration.serviceConfiguration().cdcConfiguration();
        assertThat(cdcConfig.segmentHardLinkCacheExpiry().quantity()).isEqualTo(1);
        assertThat(cdcConfig.segmentHardLinkCacheExpiry().unit()).isEqualTo(TimeUnit.MINUTES);
        assertThat(cdcConfig.isEnabled()).isEqualTo(true);
        assertThat(cdcConfig.cdcConfigRefreshTime()).isEqualTo(MillisecondBoundConfiguration.parse("10s"));
    }

    @Test
    void testAccessControlConfiguration() throws Exception
    {
        Path yamlPath = yaml("config/sidecar_multiple_instances.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);

        AccessControlConfiguration accessControlConfiguration = config.accessControlConfiguration();
        assertThat(accessControlConfiguration).isNotNull();
        assertThat(accessControlConfiguration.enabled()).isTrue();

        List<ParameterizedClassConfiguration> authenticators = accessControlConfiguration.authenticatorsConfiguration();
        assertThat(authenticators).isNotNull().hasSize(2);
        assertThat(authenticators.get(0).className()).isEqualTo("org.apache.cassandra.sidecar.acl.authentication.MutualTLSAuthenticationHandlerFactory");
        assertThat(authenticators.get(0).namedParameters())
        .contains(entry("certificate_validator", "io.vertx.ext.auth.mtls.impl.AllowAllCertificateValidator"),
                  entry("certificate_identity_extractor", "org.apache.cassandra.sidecar.acl.authentication.CassandraIdentityExtractor"));
        assertThat(authenticators.get(1).className()).isEqualTo("org.apache.cassandra.sidecar.acl.authentication.JwtAuthenticationHandlerFactory");
        assertThat(authenticators.get(1).namedParameters())
        .contains(entry("site", "https://authorization.com"),
                  entry("client_id", "recognized_client_id"),
                  entry("config_discover_interval", "1m"));

        ParameterizedClassConfiguration authorizer = accessControlConfiguration.authorizerConfiguration();
        assertThat(authorizer.className()).isEqualTo("org.apache.cassandra.sidecar.acl.authorization.RoleBasedAuthorizationProvider");

        assertThat(accessControlConfiguration.adminIdentities().size()).isEqualTo(2);
        assertThat(accessControlConfiguration.adminIdentities()).contains("spiffe://authorized/admin/identity1");
        assertThat(accessControlConfiguration.adminIdentities()).contains("spiffe://authorized/admin/identity2");

        assertThat(accessControlConfiguration.permissionCacheConfiguration()).isNotNull();
        CacheConfiguration permissionCacheConfiguration = accessControlConfiguration.permissionCacheConfiguration();
        assertThat(permissionCacheConfiguration.enabled()).isTrue();
        assertThat(permissionCacheConfiguration.expireAfterAccess().quantity()).isEqualTo(5);
        assertThat(permissionCacheConfiguration.expireAfterAccess().unit()).isEqualTo(TimeUnit.MINUTES);
        assertThat(permissionCacheConfiguration.refreshAfterWrite()).isNull();
        assertThat(permissionCacheConfiguration.maximumSize()).isEqualTo(1000);
        assertThat(permissionCacheConfiguration.warmupRetries()).isEqualTo(5);
        assertThat(permissionCacheConfiguration.warmupRetryInterval().quantity()).isEqualTo(2);
        assertThat(permissionCacheConfiguration.warmupRetryInterval().unit()).isEqualTo(TimeUnit.SECONDS);
        assertThat(permissionCacheConfiguration.warmupRetryInterval().toMillis()).isEqualTo(2_000L);
    }

    @Test
    void testCoordinationConfiguration() throws Exception
    {
        String yaml = "sidecar:\n" +
                      "  coordination:\n" +
                      "    cluster_lease_claim:\n" +
                      "      electorate_membership_strategy: SidecarInternalTokenZeroElectorateMembership\n" +
                      "      enabled: false\n" +
                      "      initial_delay: 5s\n" +
                      "      execute_interval: 31s\n" +
                      "      initial_delay_random_delta: 10s";
        SidecarConfiguration config = SidecarConfigurationImpl.fromYamlString(yaml);
        ServiceConfiguration serviceConfiguration = config.serviceConfiguration();
        assertThat(serviceConfiguration).isNotNull();

        CoordinationConfiguration coordinationConfiguration = serviceConfiguration.coordinationConfiguration();
        assertThat(coordinationConfiguration).isNotNull();
        ClusterLeaseClaimConfiguration clusterLeaseConfig = coordinationConfiguration.clusterLeaseClaimConfiguration();
        assertThat(clusterLeaseConfig.enabled()).isFalse();
        assertThat(clusterLeaseConfig.initialDelay().toMillis()).isEqualTo(5_000L);
        assertThat(clusterLeaseConfig.executeInterval().toMillis()).isEqualTo(31_000L);
        assertThat(clusterLeaseConfig.initialDelayRandomDelta().toMillis()).isEqualTo(10_000L);
        assertThat(clusterLeaseConfig.randomDeltaDelayMillis()).isBetween(0L, 10_000L);
        assertThat(clusterLeaseConfig.electorateMembershipStrategy()).isEqualTo("SidecarInternalTokenZeroElectorateMembership");
    }

    @Test
    void testCoordinationDefaultElectorateMembershipStrategy() throws Exception
    {
        String yaml = "sidecar:\n" +
                      "  coordination:\n" +
                      "    cluster_lease_claim:\n" +
                      "      enabled: false\n" +
                      "      initial_delay: 5s\n" +
                      "      execute_interval: 31s";
        SidecarConfiguration config = SidecarConfigurationImpl.fromYamlString(yaml);
        ServiceConfiguration serviceConfiguration = config.serviceConfiguration();
        assertThat(serviceConfiguration).isNotNull();

        CoordinationConfiguration coordinationConfiguration = serviceConfiguration.coordinationConfiguration();
        assertThat(coordinationConfiguration).isNotNull();
        ClusterLeaseClaimConfiguration clusterLeaseConfig = coordinationConfiguration.clusterLeaseClaimConfiguration();
        assertThat(clusterLeaseConfig.enabled()).isFalse();
        assertThat(clusterLeaseConfig.initialDelay().toMillis()).isEqualTo(5_000L);
        assertThat(clusterLeaseConfig.executeInterval().toMillis()).isEqualTo(31_000L);
        assertThat(clusterLeaseConfig.electorateMembershipStrategy()).isEqualTo("MostReplicatedKeyspaceTokenZeroElectorateMembership");
    }

    @Test
    void testLifecycle() throws Exception
    {
        String yaml = "lifecycle:\n" +
                      "  enabled: true\n" +
                      "  provider:\n" +
                      "    class_name: org.apache.cassandra.sidecar.lifecycle.ProcessLifecycleProvider\n" +
                      "    parameters:\n" +
                      "      state_dir: /var/lib/cassandra-sidecar/lifecycle\n" +
                      "      cassandra_home: /opt/cassandra\n" +
                      "      sys.cassandra.ring_delay_ms: 30000\n" +
                      "      sys.com.sun.management.jmxremote.authenticate: true\n" +
                      "      env.MAX_HEAP_SIZE: 7G\n" +
                      "      env.HEAP_NEWSIZE: 1G";
        SidecarConfiguration config = SidecarConfigurationImpl.fromYamlString(yaml);
        LifecycleConfiguration lifecycleConfiguration = config.lifecycleConfiguration();
        assertThat(lifecycleConfiguration).isNotNull();

        assertThat(lifecycleConfiguration.enabled()).isTrue();
        ParameterizedClassConfiguration provider = lifecycleConfiguration.lifecycleProvider();
        assertThat(provider.className()).isEqualTo("org.apache.cassandra.sidecar.lifecycle.ProcessLifecycleProvider");
        assertThat(provider.namedParameters()).containsEntry("state_dir", "/var/lib/cassandra-sidecar/lifecycle")
                                              .containsEntry("cassandra_home", "/opt/cassandra")
                                              .containsEntry("sys.cassandra.ring_delay_ms", "30000")
                                              .containsEntry("sys.com.sun.management.jmxremote.authenticate", "true")
                                              .containsEntry("env.MAX_HEAP_SIZE", "7G")
                                              .containsEntry("env.HEAP_NEWSIZE", "1G");
    }

    @Test
    void testDnsResolverDefault() throws Exception
    {
        Path yamlPath = yaml("config/sidecar_single_instance.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);
        ServiceConfiguration serviceConfiguration = config.serviceConfiguration();
        assertThat(serviceConfiguration).isNotNull();

        DnsResolver dnsResolver = serviceConfiguration.dnsResolver();
        assertThat(dnsResolver).isNotNull();
        assertThat(dnsResolver.resolve("localhost")).isEqualTo("127.0.0.1");
        assertThat(dnsResolver.reverseResolve("127.0.0.1")).isEqualTo("localhost");
    }

    @Test
    void testDnsResolverResolveToIp() throws Exception
    {
        String yaml = "sidecar:\n" +
                      "  dns_resolver: resolve_to_ip";
        SidecarConfiguration config = SidecarConfigurationImpl.fromYamlString(yaml);
        ServiceConfiguration serviceConfiguration = config.serviceConfiguration();
        assertThat(serviceConfiguration).isNotNull();

        DnsResolver dnsResolver = serviceConfiguration.dnsResolver();
        assertThat(dnsResolver).isNotNull();
        assertThat(dnsResolver.resolve("localhost")).isEqualTo("127.0.0.1");
        assertThat(dnsResolver.reverseResolve("127.0.0.1")).isEqualTo("127.0.0.1");
    }

    @Test
    void testSidecarLiveMigrationConfiguration() throws IOException
    {
        Path yamlPath = yaml("config/sidecar_live_migration.yaml");
        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(yamlPath);
        LiveMigrationConfiguration liveMigrationConfiguration = config.liveMigrationConfiguration();
        assertThat(liveMigrationConfiguration).isNotNull();

        assertThat(liveMigrationConfiguration.filesToExclude()).isNotNull()
                                                               .hasSize(1)
                                                               .contains("${DATA_FILE_DIR}/test.log");

        assertThat(liveMigrationConfiguration.directoriesToExclude()).isNotNull()
                                                                     .hasSize(1)
                                                                     .contains("${DATA_FILE_DIR}/*/*/snapshots");

        assertThat(liveMigrationConfiguration.migrationMap()).isNotNull()
                                                             .hasSize(1)
                                                             .containsEntry("localhost1", "localhost4");
    }

    void validateSingleInstanceSidecarConfiguration(SidecarConfiguration config)
    {
        assertThat(config.cassandraInstances()).isNotNull().hasSize(1);

        InstanceConfiguration i1 = config.cassandraInstances().get(0);

        // instance 1
        assertThat(i1.id()).isEqualTo(0);
        assertThat(i1.host()).isEqualTo("localhost");
        assertThat(i1.port()).isEqualTo(9042);
        assertThat(i1.dataDirs()).containsExactly("/ccm/test/node1/data0", "/ccm/test/node1/data1");
        assertThat(i1.stagingDir()).isEqualTo("/ccm/test/node1/sstable-staging");
        assertThat(i1.jmxHost()).isEqualTo("127.0.0.1");
        assertThat(i1.jmxPort()).isEqualTo(7199);
        assertThat(i1.jmxSslEnabled()).isTrue();
        assertThat(i1.jmxRole()).isEqualTo("controlRole");
        assertThat(i1.jmxRolePassword()).isEqualTo("controlPassword");

        // service configuration
        validateServiceConfigurationFromYaml(config.serviceConfiguration());

        // ssl configuration
        assertThat(config.sslConfiguration()).isNull();

        // health check configuration
        validateHealthCheckConfigurationFromYaml(config.healthCheckConfiguration());

        // metrics configuration
        validateMetricsConfiguration(config.metricsConfiguration());

        validateSidecarPeerHealthConfigurationDefaults(config.sidecarPeerHealthConfiguration());

        // cassandra input validation configuration
        validateCassandraInputValidationConfigurationFromYaml(config.cassandraInputValidationConfiguration());

        // vertx FileSystemOptions
        VertxConfiguration vertxConfiguration = config.vertxConfiguration();
        assertThat(vertxConfiguration).isNotNull();
        validateVertxFilesystemOptionsClasspathResolvingDisabled(vertxConfiguration.filesystemOptionsConfiguration());
    }

    void validateMultipleInstancesSidecarConfiguration(SidecarConfiguration config, boolean withSslConfiguration)
    {
        // instances configuration
        assertThat(config.cassandraInstances()).isNotNull().hasSize(3);

        InstanceConfiguration i1 = config.cassandraInstances().get(0);
        InstanceConfiguration i2 = config.cassandraInstances().get(1);
        InstanceConfiguration i3 = config.cassandraInstances().get(2);

        // instance 1
        assertThat(i1.id()).isEqualTo(1);
        assertThat(i1.host()).isEqualTo("localhost1");
        assertThat(i1.port()).isEqualTo(9042);
        assertThat(i1.dataDirs()).containsExactly("/ccm/test/node1/data0", "/ccm/test/node1/data1");
        assertThat(i1.stagingDir()).isEqualTo("/ccm/test/node1/sstable-staging");
        assertThat(i1.jmxHost()).isEqualTo("127.0.0.1");
        assertThat(i1.jmxPort()).isEqualTo(7100);
        assertThat(i1.jmxSslEnabled()).isFalse();
        assertThat(i1.commitlogDir()).isEqualTo("/ccm/test/node1/commitlog");
        assertThat(i1.hintsDir()).isEqualTo("/ccm/test/node1/hints");
        assertThat(i1.savedCachesDir()).isEqualTo("/ccm/test/node1/saved_caches");
        assertThat(i1.localSystemDataFileDir()).isEqualTo("/ccm/test/node1/local_system_data");

        // instance 2
        assertThat(i2.id()).isEqualTo(2);
        assertThat(i2.host()).isEqualTo("localhost2");
        assertThat(i2.port()).isEqualTo(9042);
        assertThat(i2.dataDirs()).containsExactly("/ccm/test/node2/data0", "/ccm/test/node2/data1");
        assertThat(i2.stagingDir()).isEqualTo("/ccm/test/node2/sstable-staging");
        assertThat(i2.jmxHost()).isEqualTo("127.0.0.1");
        assertThat(i2.jmxPort()).isEqualTo(7200);
        assertThat(i2.jmxSslEnabled()).isFalse();
        assertThat(i2.commitlogDir()).isEqualTo("/ccm/test/node2/commitlog");
        assertThat(i2.hintsDir()).isEqualTo("/ccm/test/node2/hints");
        assertThat(i2.savedCachesDir()).isEqualTo("/ccm/test/node2/saved_caches");
        assertThat(i2.localSystemDataFileDir()).isEqualTo("/ccm/test/node2/local_system_data");

        // instance 3
        assertThat(i3.id()).isEqualTo(3);
        assertThat(i3.host()).isEqualTo("localhost3");
        assertThat(i3.port()).isEqualTo(9042);
        assertThat(i3.dataDirs()).containsExactly("/ccm/test/node3/data0", "/ccm/test/node3/data1");
        assertThat(i3.stagingDir()).isEqualTo("/ccm/test/node3/sstable-staging");
        assertThat(i3.jmxHost()).isEqualTo("127.0.0.1");
        assertThat(i3.jmxPort()).isEqualTo(7300);
        assertThat(i3.jmxSslEnabled()).isFalse();
        assertThat(i3.commitlogDir()).isEqualTo("/ccm/test/node3/commitlog");
        assertThat(i3.hintsDir()).isEqualTo("/ccm/test/node3/hints");
        assertThat(i3.savedCachesDir()).isEqualTo("/ccm/test/node3/saved_caches");
        assertThat(i3.localSystemDataFileDir()).isEqualTo("/ccm/test/node3/local_system_data");

        // service configuration
        validateServiceConfigurationFromYaml(config.serviceConfiguration());

        // ssl configuration
        if (withSslConfiguration)
        {
            validateSslConfigurationFromYaml(config.sslConfiguration());
        }
        else
        {
            assertThat(config.sslConfiguration()).isNull();
        }

        // health check configuration
        validateHealthCheckConfigurationFromYaml(config.healthCheckConfiguration());

        // metrics configuration
        validateMetricsConfiguration(config.metricsConfiguration());

        // cassandra input validation configuration
        validateCassandraInputValidationConfigurationFromYaml(config.cassandraInputValidationConfiguration());
    }

    private void validateSidecarPeerHealthConfigurationDefaults(SidecarPeerHealthConfiguration config)
    {
        assertThat(config).isNotNull();
        assertThat(config.enabled()).isFalse();
        assertThat(config.executeInterval().toMillis()).isEqualTo(30_000);
        assertThat(config.maxRetries()).isEqualTo(5);
        assertThat(config.retryDelay().toMillis()).isEqualTo(10_000);
    }

    void validateServiceConfigurationFromYaml(ServiceConfiguration serviceConfiguration)
    {
        assertThat(serviceConfiguration).isNotNull();
        assertThat(serviceConfiguration.host()).isEqualTo("0.0.0.0");
        assertThat(serviceConfiguration.port()).is(new Condition<>(port -> port == 9043 || port == 0, "port"));
        assertThat(serviceConfiguration.requestIdleTimeout().quantity()).isEqualTo(5);
        assertThat(serviceConfiguration.requestIdleTimeout().unit()).isEqualTo(TimeUnit.MINUTES);
        assertThat(serviceConfiguration.requestIdleTimeout().toMillis()).isEqualTo(300_000);
        assertThat(serviceConfiguration.requestTimeout().quantity()).isEqualTo(5);
        assertThat(serviceConfiguration.requestTimeout().unit()).isEqualTo(TimeUnit.MINUTES);
        assertThat(serviceConfiguration.requestTimeout().toMillis()).isEqualTo(300_000);
        assertThat(serviceConfiguration.allowableTimeSkew().quantity()).isEqualTo(1);
        assertThat(serviceConfiguration.allowableTimeSkew().unit()).isEqualTo(TimeUnit.HOURS);
        assertThat(serviceConfiguration.allowableTimeSkew().toSeconds()).isEqualTo(3_600);
        assertThat(serviceConfiguration.tcpKeepAlive()).isFalse();
        assertThat(serviceConfiguration.acceptBacklog()).isEqualTo(1024);

        // service configuration throttling
        ThrottleConfiguration throttle = serviceConfiguration.throttleConfiguration();

        assertThat(throttle).isNotNull();
        assertThat(throttle.rateLimitStreamRequestsPerSecond()).isEqualTo(5000);
        assertThat(throttle.timeout().quantity()).isEqualTo(10);
        assertThat(throttle.timeout().unit()).isEqualTo(TimeUnit.SECONDS);
        assertThat(throttle.timeout().toSeconds()).isEqualTo(10);

        // sstable import configuration
        SSTableImportConfiguration importConfig = serviceConfiguration.sstableImportConfiguration();
        assertThat(importConfig).isNotNull();
        assertThat(importConfig.executeInterval().quantity()).isEqualTo(100);
        assertThat(importConfig.executeInterval().unit()).isEqualTo(TimeUnit.MILLISECONDS);

        // validate traffic shaping options
        TrafficShapingConfiguration trafficShaping = serviceConfiguration.trafficShapingConfiguration();
        assertThat(trafficShaping).isNotNull();
        assertThat(trafficShaping.inboundGlobalBandwidthBytesPerSecond()).isEqualTo(500L);
        assertThat(trafficShaping.outboundGlobalBandwidthBytesPerSecond()).isEqualTo(1500L);
        assertThat(trafficShaping.peakOutboundGlobalBandwidthBytesPerSecond()).isEqualTo(2000L);
        assertThat(trafficShaping.maxDelayToWait().quantity()).isEqualTo(15);
        assertThat(trafficShaping.maxDelayToWait().unit()).isEqualTo(TimeUnit.SECONDS);
        assertThat(trafficShaping.maxDelayToWait().toMillis()).isEqualTo(15_000L);
        assertThat(trafficShaping.checkIntervalForStats().quantity()).isEqualTo(1);
        assertThat(trafficShaping.checkIntervalForStats().unit()).isEqualTo(TimeUnit.SECONDS);
        assertThat(trafficShaping.checkIntervalForStats().toMillis()).isEqualTo(1_000L);

        // SSTable snapshot configuration section
        SSTableSnapshotConfiguration snapshotConfig = serviceConfiguration.sstableSnapshotConfiguration();

        assertThat(snapshotConfig).isNotNull();

        assertThat(snapshotConfig.snapshotListCacheConfiguration().enabled()).isTrue();
        assertThat(snapshotConfig.snapshotListCacheConfiguration().maximumSize()).isEqualTo(450);
        assertThat(snapshotConfig.snapshotListCacheConfiguration().expireAfterAccess().quantity()).isEqualTo(350);
        assertThat(snapshotConfig.snapshotListCacheConfiguration().expireAfterAccess().unit()).isEqualTo(TimeUnit.MILLISECONDS);

        // Validate default configuration
        CoordinationConfiguration coordinationConfiguration = serviceConfiguration.coordinationConfiguration();
        assertThat(coordinationConfiguration).isNotNull();
        ClusterLeaseClaimConfiguration clusterLeaseConfig = coordinationConfiguration.clusterLeaseClaimConfiguration();
        assertThat(clusterLeaseConfig.enabled()).isTrue();
        assertThat(clusterLeaseConfig.executeInterval().toMillis()).isEqualTo(100_000L);
        assertThat(clusterLeaseConfig.initialDelay().toMillis()).isEqualTo(1_000L);
        assertThat(clusterLeaseConfig.initialDelayRandomDelta().toMillis()).isEqualTo(30_000L);
        assertThat(clusterLeaseConfig.randomDeltaDelayMillis()).isBetween(0L, 30_000L);
        assertThat(clusterLeaseConfig.electorateMembershipStrategy()).isEqualTo("MostReplicatedKeyspaceTokenZeroElectorateMembership");
    }

    private void validateHealthCheckConfigurationFromYaml(PeriodicTaskConfiguration config)
    {
        assertThat(config).isNotNull();
        assertThat(config.initialDelay().quantity()).isEqualTo(100);
        assertThat(config.initialDelay().unit()).isEqualTo(TimeUnit.MILLISECONDS);
        assertThat(config.initialDelay().toMillis()).isEqualTo(100L);
        assertThat(config.executeInterval().quantity()).isEqualTo(30);
        assertThat(config.executeInterval().unit()).isEqualTo(TimeUnit.SECONDS);
        assertThat(config.executeInterval().toMillis()).isEqualTo(30_000);
    }

    void validateCassandraInputValidationConfigurationFromYaml(CassandraInputValidationConfiguration config)
    {
        assertThat(config).isNotNull();
        assertThat(config.forbiddenKeyspaces()).containsExactlyInAnyOrder("system_schema",
                                                                          "system_traces",
                                                                          "system_distributed",
                                                                          "system",
                                                                          "system_auth",
                                                                          "system_views",
                                                                          "system_virtual_schema",
                                                                          "sidecar_internal");
        assertThat(config.allowedPatternForName()).isEqualTo("[a-zA-Z][a-zA-Z0-9_]{0,47}");
        assertThat(config.allowedPatternForQuotedName()).isEqualTo("[a-zA-Z_0-9]{1,48}");
        assertThat(config.allowedPatternForComponentName())
        .isEqualTo("[a-zA-Z0-9_-]+(\\.db|\\.cql|\\.json|\\.crc32|TOC\\.txt)");
        assertThat(config.allowedPatternForRestrictedComponentName()).isEqualTo("[a-zA-Z0-9_-]+(\\.db|TOC\\.txt)");
    }

    void validateVertxFilesystemOptionsClasspathResolvingDisabled(FileSystemOptionsConfiguration config)
    {
        assertThat(config).isNotNull();
        assertThat(config.classpathResolvingEnabled()).isTrue();
        assertThat(config.fileCacheDir()).isNotNull();
        assertThat(config.fileCachingEnabled()).isNotNull();
    }

    void validateSslConfigurationFromYaml(SslConfiguration config)
    {
        assertThat(config).isNotNull();
        assertThat(config.enabled()).isTrue();
        assertThat(config.preferOpenSSL()).isFalse();
        assertThat(config.handshakeTimeout().quantity()).isEqualTo(25);
        assertThat(config.handshakeTimeout().unit()).isEqualTo(TimeUnit.SECONDS);
        assertThat(config.handshakeTimeout().toSeconds()).isEqualTo(25);
        assertThat(config.clientAuth()).isEqualTo("REQUEST");
        assertThat(config.keystore()).isNotNull();
        assertThat(config.keystore().type()).isEqualTo("PKCS12");
        assertThat(config.keystore().path()).isEqualTo("path/to/keystore.p12");
        assertThat(config.keystore().password()).isEqualTo("password");
        assertThat(config.keystore().reloadStore()).isTrue();
        assertThat(config.keystore().checkInterval().quantity()).isEqualTo(5);
        assertThat(config.keystore().checkInterval().unit()).isEqualTo(TimeUnit.MINUTES);
        assertThat(config.truststore()).isNotNull();
        assertThat(config.truststore().path()).isEqualTo("path/to/truststore.p12");
        assertThat(config.truststore().password()).isEqualTo("password");
        assertThat(config.truststore().reloadStore()).isFalse();
        assertThat(config.truststore().checkInterval().quantity()).isEqualTo(0);
        assertThat(config.truststore().checkInterval().unit()).isEqualTo(TimeUnit.SECONDS);
        assertThat(config.secureTransportProtocols()).containsExactly("TLSv1.3");
        assertThat(config.cipherSuites()).contains("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
                                                   "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
                                                   "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
                                                   "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
                                                   "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
                                                   "TLS_RSA_WITH_AES_128_GCM_SHA256",
                                                   "TLS_RSA_WITH_AES_128_CBC_SHA",
                                                   "TLS_RSA_WITH_AES_256_CBC_SHA");
    }

    void validateMetricsConfiguration(MetricsConfiguration config)
    {
        assertThat(config.registryName()).isNotEmpty();
        assertThat(config.vertxConfiguration()).isNotNull();
        assertThat(config.includeConfigurations()).isNotNull();
        assertThat(config.excludeConfigurations()).isNotNull();
    }

    private Path yaml(String resourceName)
    {
        ClassLoader classLoader = this.getClass().getClassLoader();
        return writeResourceToPath(classLoader, configPath, resourceName);
    }
}

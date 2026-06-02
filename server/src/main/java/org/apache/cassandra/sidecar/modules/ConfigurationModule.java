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

package org.apache.cassandra.sidecar.modules;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.SidecarRateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.NettyOptions;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoMap;
import com.google.inject.name.Named;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.adapters.base.CassandraFactory;
import org.apache.cassandra.sidecar.adapters.cassandra41.Cassandra41Factory;
import org.apache.cassandra.sidecar.adapters.cassandra50.Cassandra50Factory;
import org.apache.cassandra.sidecar.cluster.CQLSessionProviderImpl;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.InstancesMetadataImpl;
import org.apache.cassandra.sidecar.cluster.auth.ConfigProvider;
import org.apache.cassandra.sidecar.cluster.auth.CqlAuthProvider;
import org.apache.cassandra.sidecar.cluster.auth.FileProvider;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.JmxClient;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.server.utils.DriverUtils;
import org.apache.cassandra.sidecar.common.server.utils.SidecarVersionProvider;
import org.apache.cassandra.sidecar.config.CassandraInputValidationConfiguration;
import org.apache.cassandra.sidecar.config.DriverConfiguration;
import org.apache.cassandra.sidecar.config.InstanceConfiguration;
import org.apache.cassandra.sidecar.config.JmxConfiguration;
import org.apache.cassandra.sidecar.config.ParameterizedClassConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.DriverUnsupportedSchemaCache;
import org.apache.cassandra.sidecar.db.schema.TableSchemaFetcher;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.apache.cassandra.sidecar.metrics.MetricRegistryFactory;
import org.apache.cassandra.sidecar.metrics.instance.InstanceHealthMetrics;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.PeriodicTaskMapKeys;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.utils.CassandraVersionProvider;
import org.apache.cassandra.sidecar.utils.EventBusUtils;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.sidecar.common.server.utils.ByteUtils.bytesToHumanReadableBinaryPrefix;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CASSANDRA_CQL_READY;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SERVER_STOP;

/**
 * Provides various configurations required by Sidecar
 */
public class ConfigurationModule extends AbstractModule
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationModule.class);
    @Nullable
    private final SidecarConfiguration config;


    /**
     * Constructs the Guice main module with a pre-parsed {@link SidecarConfiguration}.
     * Using this constructor avoids a second YAML parse at injection time.
     *
     * @param config the already-parsed sidecar configuration
     */
    public ConfigurationModule(SidecarConfiguration config)
    {
        this.config = config;
    }

    @Provides
    @Singleton
    SidecarConfiguration sidecarConfiguration()
    {
        if (config != null)
        {
            return config;
        }
        throw new NullPointerException("Sidecar configuration is not defined");
    }

    @Provides
    @Singleton
    ServiceConfiguration serviceConfiguration(SidecarConfiguration sidecarConfiguration)
    {
        return sidecarConfiguration.serviceConfiguration();
    }

    @Provides
    @Singleton
    CassandraInputValidationConfiguration validationConfiguration(SidecarConfiguration configuration)
    {
        return configuration.cassandraInputValidationConfiguration();
    }

    @Provides
    @Singleton
    CQLSessionProvider cqlSessionProvider(Vertx vertx,
                                          SidecarConfiguration sidecarConfiguration,
                                          CqlAuthProvider cqlAuthProvider,
                                          DriverUtils driverUtils)
    {
        CQLSessionProviderImpl cqlSessionProvider = new CQLSessionProviderImpl(sidecarConfiguration,
                                                                               NettyOptions.DEFAULT_INSTANCE,
                                                                               cqlAuthProvider,
                                                                               driverUtils);
        vertx.eventBus().localConsumer(ON_SERVER_STOP.address(), message -> cqlSessionProvider.close());
        return cqlSessionProvider;
    }

    @Provides
    @Singleton
    CqlAuthProvider cqlAuthProvider(SidecarConfiguration sidecarConfiguration)
    {
        DriverConfiguration driverConfiguration = sidecarConfiguration.driverConfiguration();

        ParameterizedClassConfiguration config = driverConfiguration.authProvider();
        if (config == null)
        {
            // Fallback to the old one
            Map<String, String> namedParameters = new HashMap<>();
            namedParameters.put("username", driverConfiguration.username());
            namedParameters.put("password", driverConfiguration.password());
            return new ConfigProvider(namedParameters);
        }

        if (config.namedParameters() == null)
        {
            throw new ConfigurationException("Missing parameters for auth_provider");
        }

        Map<String, String> namedParameters = config.namedParameters();

        if (config.className().equalsIgnoreCase(ConfigProvider.class.getName()))
        {
            return new ConfigProvider(namedParameters);
        }
        if (config.className().equalsIgnoreCase(FileProvider.class.getName()))
        {
            return new FileProvider(namedParameters);
        }
        throw new ConfigurationException("Unrecognized cql auth_provider " + config.className() + " set");
    }

    @Provides
    @Singleton
    DriverUnsupportedSchemaCache driverUnsupportedSchemaCache(Vertx vertx,
                                                              SidecarConfiguration sidecarConfiguration,
                                                              CQLSessionProvider cqlSessionProvider)
    {
        DriverUnsupportedSchemaCache schemaCache = new DriverUnsupportedSchemaCache(sidecarConfiguration, cqlSessionProvider);
        // trigger immediate cache population once Sidecar connects to Cassandra node
        EventBusUtils.onceLocalConsumer(vertx.eventBus(),
                                        ON_CASSANDRA_CQL_READY.address(),
                                        ignored -> vertx.executeBlocking(() -> {
                                            schemaCache.refresh(true);
                                            return null;
                                        }));
        return schemaCache;
    }

    @ProvidesIntoMap
    @KeyClassMapKey(PeriodicTaskMapKeys.UnsupportedSchemaCacheTaskKey.class)
    PeriodicTask driverUnsupportedSchemaCachePeriodicTask(DriverUnsupportedSchemaCache schemaCache)
    {
        return schemaCache;
    }

    @Provides
    @Singleton
    CassandraVersionProvider cassandraVersionProvider(DnsResolver dnsResolver, DriverUtils driverUtils, TableSchemaFetcher tableSchemaFetcher)
    {
        return new CassandraVersionProvider.Builder()
               .add(new CassandraFactory(dnsResolver, driverUtils, tableSchemaFetcher))
               .add(new Cassandra41Factory(dnsResolver, driverUtils, tableSchemaFetcher))
                .add(new Cassandra50Factory(dnsResolver, driverUtils, tableSchemaFetcher))
                .build();
    }

    @Provides
    @Singleton
    @Named("StreamRequestRateLimiter")
    SidecarRateLimiter streamRequestRateLimiter(ServiceConfiguration config)
    {
        long permitsPerSecond = config.throttleConfiguration().rateLimitStreamRequestsPerSecond();
        LOGGER.info("Configuring streamRequestRateLimiter. rateLimitStreamRequestsPerSecond={}",
                    permitsPerSecond);
        return SidecarRateLimiter.create(permitsPerSecond);
    }

    @Provides
    @Singleton
    @Named("IngressFileRateLimiter")
    SidecarRateLimiter ingressFileRateLimiter(ServiceConfiguration config)
    {
        long bytesPerSecond = config.trafficShapingConfiguration()
                                    .inboundGlobalFileBandwidthBytesPerSecond();
        LOGGER.info("Configuring ingressFileRateLimiter. inboundGlobalFileBandwidth={}/s " +
                    "rawInboundGlobalFileBandwidth={} B/s", bytesToHumanReadableBinaryPrefix(bytesPerSecond),
                    bytesPerSecond);
        return SidecarRateLimiter.create(bytesPerSecond);
    }

    @Provides
    @Singleton
    InstancesMetadata instancesMetadata(Vertx vertx,
                                        SidecarConfiguration configuration,
                                        CassandraVersionProvider cassandraVersionProvider,
                                        SidecarVersionProvider sidecarVersionProvider,
                                        DnsResolver dnsResolver,
                                        CQLSessionProvider cqlSessionProvider,
                                        DriverUtils driverUtils,
                                        MetricRegistryFactory registryProvider)
    {
        List<InstanceMetadata> instanceMetadataList =
        configuration.cassandraInstances()
                     .stream()
                     .map(cassandraInstance -> {
                         JmxConfiguration jmxConfiguration = configuration.serviceConfiguration().jmxConfiguration();
                         return buildInstanceMetadata(vertx,
                                                      cassandraInstance,
                                                      cassandraVersionProvider,
                                                      sidecarVersionProvider.sidecarVersion(),
                                                      jmxConfiguration,
                                                      cqlSessionProvider,
                                                      driverUtils,
                                                      registryProvider,
                                                      dnsResolver);
                     })
                     .collect(Collectors.toList());

        return new InstancesMetadataImpl(instanceMetadataList, dnsResolver);
    }

    /**
     * Builds the {@link InstanceMetadata} from the {@link InstanceConfiguration},
     * a provided {@code  versionProvider}, and {@code healthCheckFrequencyMillis}.
     *
     * @param vertx             the vertx instance
     * @param cassandraInstance the cassandra instance configuration
     * @param versionProvider   a Cassandra version provider
     * @param sidecarVersion    the version of the Sidecar from the current binary
     * @param jmxConfiguration  the configuration for the JMX Client
     * @param session           the CQL Session provider
     * @param registryFactory   factory for creating cassandra instance specific registry
     * @param dnsResolver       the dns resolver to use
     * @return the build instance metadata object
     */
    private static InstanceMetadata buildInstanceMetadata(Vertx vertx,
                                                          InstanceConfiguration cassandraInstance,
                                                          CassandraVersionProvider versionProvider,
                                                          String sidecarVersion,
                                                          JmxConfiguration jmxConfiguration,
                                                          CQLSessionProvider session,
                                                          DriverUtils driverUtils,
                                                          MetricRegistryFactory registryFactory,
                                                          DnsResolver dnsResolver)
    {
        // TODO: relocate the method to somewhere testable
        String host = cassandraInstance.host();
        int port = cassandraInstance.port();

        JmxClient jmxClient = JmxClient.builder()
                                       .host(cassandraInstance.jmxHost())
                                       .port(cassandraInstance.jmxPort())
                                       .role(cassandraInstance.jmxRole())
                                       .password(cassandraInstance.jmxRolePassword())
                                       .enableSsl(cassandraInstance.jmxSslEnabled())
                                       .connectionMaxRetries(jmxConfiguration.maxRetries())
                                       .connectionRetryDelay(jmxConfiguration.retryDelay())
                                       .build();
        MetricRegistry instanceSpecificRegistry = registryFactory.getOrCreate(cassandraInstance.id());
        CassandraAdapterDelegate delegate = new CassandraAdapterDelegate(vertx,
                                                                         cassandraInstance.id(),
                                                                         versionProvider,
                                                                         session,
                                                                         jmxClient,
                                                                         driverUtils,
                                                                         sidecarVersion,
                                                                         host,
                                                                         port,
                                                                         new InstanceHealthMetrics(instanceSpecificRegistry));
        return InstanceMetadataImpl.builder()
                                   .id(cassandraInstance.id())
                                   .host(host, dnsResolver)
                                   .port(port)
                                   .storagePort(cassandraInstance.storagePort())
                                   .storageDir(cassandraInstance.storageDir())
                                   .dataDirs(cassandraInstance.dataDirs())
                                   .stagingDir(cassandraInstance.stagingDir())
                                   .cdcDir(cassandraInstance.cdcDir())
                                   .commitlogDir(cassandraInstance.commitlogDir())
                                   .hintsDir(cassandraInstance.hintsDir())
                                   .savedCachesDir(cassandraInstance.savedCachesDir())
                                   .localSystemDataFileDir(cassandraInstance.localSystemDataFileDir())
                                   .lifecycleOptions(cassandraInstance.lifecycleOptions())
                                   .delegate(delegate)
                                   .metricRegistry(instanceSpecificRegistry)
                                   .build();
    }
}

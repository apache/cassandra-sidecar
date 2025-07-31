package org.apache.cassandra.sidecar.handlers.v2.cassandra;

import java.util.Map;
import java.util.Set;

import com.google.inject.Inject;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.response.v2.JmxNodeSettings;
import org.apache.cassandra.sidecar.common.response.v2.V2NodeSettings;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.handlers.AbstractHandler;
import org.apache.cassandra.sidecar.handlers.AccessProtected;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

/**
 * V2NodeSettingsHandler is responsible for providing access to the configurations of the
 * Cassandra instances managed by this sidecar. This includes settings accessible through JMX
 * as well as settings stored in the system_views.settings Cassandra virtual tables. Additionally,
 * configuration information about this sidecar instance are accessible through this endpoint (e.g., Sidecar verison).
 */
public class V2NodeSettingsHandler extends AbstractHandler<Void> implements AccessProtected
{

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the interface to retrieve instance metadata
     */
    @Inject
    V2NodeSettingsHandler(InstanceMetadataFetcher metadataFetcher, ExecutorPools executorPools)
    {
        super(metadataFetcher, executorPools, null);
    }

    @Override
    protected Void extractParamsOrThrow(RoutingContext context)
    {
        return null;
    }

    @Override
    protected void handleInternal(RoutingContext context, HttpServerRequest httpRequest, @NotNull String host, SocketAddress remoteAddress, Void request)
    {
        NodeSettings nodeSettings = metadataFetcher.delegate(host).nodeSettings();
        Map<String, String> cqlSettings = metadataFetcher.delegate(host).cqlNodeSettings();
        Map<String, String> sidecarSettings = nodeSettings.sidecar();
        JmxNodeSettings jmxNodeSettings = JmxNodeSettings.builder()
                                                         .releaseVersion(nodeSettings.releaseVersion())
                                                         .partitioner(nodeSettings.partitioner())
                                                         .datacenter(nodeSettings.datacenter())
                                                         .rpcAddress(nodeSettings.rpcAddress())
                                                         .rpcPort(nodeSettings.rpcPort())
                                                         .tokens(nodeSettings.tokens())
                                                         .build();
        V2NodeSettings v2nodeSettings = V2NodeSettings.builder()
                                                    .cassandra(cqlSettings)
                                                    .sidecar(sidecarSettings)
                                                    .jmx(jmxNodeSettings)
                                                    .build();
        context.json(v2nodeSettings);
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        Authorization authorization = BasicPermissions.READ_SETTINGS.toAuthorization("data/system_views/settings");
        return Set.of(authorization);
    }
}

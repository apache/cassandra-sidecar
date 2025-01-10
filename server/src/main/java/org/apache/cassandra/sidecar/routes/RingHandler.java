package org.apache.cassandra.sidecar.routes;

import java.util.Collections;
import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.SidecarPermissions;
import org.apache.cassandra.sidecar.acl.authorization.VariableAwareResource;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

/**
 * A handler that provides ring information for a specific keyspace for the Cassandra cluster
 */
@Singleton
public class RingHandler extends KeyspaceRingHandler
{
    @Inject
    public RingHandler(InstanceMetadataFetcher metadataFetcher,
                       CassandraInputValidator validator,
                       ExecutorPools executorPools)
    {
        super(metadataFetcher, validator, executorPools);
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        String resource = VariableAwareResource.CLUSTER.resource();
        return Collections.singleton(SidecarPermissions.READ_RING.toAuthorization(resource));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Name extractParamsOrThrow(RoutingContext context)
    {
        return null;
    }
}

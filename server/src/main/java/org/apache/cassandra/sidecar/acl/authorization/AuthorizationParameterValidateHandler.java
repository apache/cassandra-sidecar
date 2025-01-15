package org.apache.cassandra.sidecar.acl.authorization;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.routes.AbstractHandler;
import org.apache.cassandra.sidecar.routes.RoutingContextUtils;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

/**
 * Simple handler that extracts authorization parameters keyspace/table parameters from the path, validates them,
 * and then adds them to the context.
 */
@Singleton
public class AuthorizationParameterValidateHandler extends AbstractHandler<QualifiedTableName>
{
    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the interface to retrieve instance metadata
     * @param executorPools   the executor pools for blocking executions
     * @param validator       a validator instance to validate Cassandra-specific input
     */
    @Inject
    protected AuthorizationParameterValidateHandler(InstanceMetadataFetcher metadataFetcher,
                                                    ExecutorPools executorPools,
                                                    CassandraInputValidator validator)
    {
        super(metadataFetcher, executorPools, validator);
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  String host,
                                  SocketAddress remoteAddress,
                                  QualifiedTableName qualifiedTableName)
    {
        RoutingContextUtils.put(context, RoutingContextUtils.SC_QUALIFIED_TABLE_NAME, qualifiedTableName);
    }

    @Override
    protected QualifiedTableName extractParamsOrThrow(RoutingContext context)
    {
        return qualifiedTableName(context, false);
    }
}

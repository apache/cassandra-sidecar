package org.apache.cassandra.sidecar.routes;

import java.io.FileNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.common.data.StreamSSTableComponentRequest;
import org.apache.cassandra.sidecar.snapshots.SnapshotPathBuilder;

import static org.apache.cassandra.sidecar.utils.RequestUtils.extractHostAddressWithoutPort;

/**
 * This handler validates that the component exists in the cluster and sets up the context
 * for the {@link FileStreamHandler} to stream the component back to the client
 */
@Singleton
public class StreamSSTableComponentHandler
{
    private static final Logger logger = LoggerFactory.getLogger(StreamSSTableComponentHandler.class);

    private final SnapshotPathBuilder snapshotPathBuilder;
    private final InstancesConfig instancesConfig;

    @Inject
    public StreamSSTableComponentHandler(SnapshotPathBuilder snapshotPathBuilder, InstancesConfig instancesConfig)
    {
        this.snapshotPathBuilder = snapshotPathBuilder;
        this.instancesConfig = instancesConfig;
    }

    public void handleAllRequests(RoutingContext context)
    {
        final HttpServerRequest request = context.request();
        final String host = extractHostAddressWithoutPort(request.host());
        streamFilesForHost(host, context);
    }

    public void handlePerInstanceRequests(RoutingContext context)
    {
        final String instanceIdParam = context.request().getParam("InstanceId");
        if (instanceIdParam == null)
        {
            context.fail(new HttpException(HttpResponseStatus.BAD_REQUEST.code(),
                                           "InstanceId path parameter must be provided"));
            return;
        }

        final Integer instanceId = Integer.valueOf(instanceIdParam);
        final String host = instancesConfig.instanceFromId(instanceId).host();
        streamFilesForHost(host, context);
    }

    public void streamFilesForHost(String host, RoutingContext context)
    {
        final SocketAddress remoteAddress = context.request().remoteAddress();
        final StreamSSTableComponentRequest requestParams = extractParamsOrThrow(context);
        logger.info("StreamSSTableComponentHandler received request: {} from: {}. Instance: {}", requestParams,
                    remoteAddress, host);

        snapshotPathBuilder.build(host, requestParams)
                           .onSuccess(path ->
               {
                   logger.debug("StreamSSTableComponentHandler handled {} for client {}. Instance: {}", path,
                                remoteAddress, host);
                   context.put(FileStreamHandler.FILE_PATH_CONTEXT_KEY, path)
                          .next();
               })
                           .onFailure(cause ->
               {
                   if (cause instanceof FileNotFoundException)
                   {
                       context.fail(new HttpException(HttpResponseStatus.NOT_FOUND.code(), cause.getMessage()));
                   }
                   else
                   {
                       context.fail(new HttpException(HttpResponseStatus.BAD_REQUEST.code(),
                                                      "Invalid request for " + requestParams));
                   }
               });
    }

    private StreamSSTableComponentRequest extractParamsOrThrow(final RoutingContext rc)
    {
        return new StreamSSTableComponentRequest(rc.pathParam("keyspace"),
                                                 rc.pathParam("table"),
                                                 rc.pathParam("snapshot"),
                                                 rc.pathParam("component")
        );
    }
}

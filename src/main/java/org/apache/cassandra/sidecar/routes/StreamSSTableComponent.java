package org.apache.cassandra.sidecar.routes;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import org.apache.cassandra.sidecar.models.ComponentInfo;
import org.apache.cassandra.sidecar.models.HttpResponse;
import org.apache.cassandra.sidecar.models.Range;
import org.apache.cassandra.sidecar.utils.FilePathBuilder;
import org.apache.cassandra.sidecar.utils.FileStreamer;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

/**
 * Handler for serving SSTable components from snapshot folders
 */
@Singleton
@javax.ws.rs.Path("/api")
public class StreamSSTableComponent
{
    private static final Pattern REGEX_DIR = Pattern.compile("[a-zA-Z0-9_-]+");
    private static final Pattern REGEX_COMPONENT = Pattern.compile("[a-zA-Z0-9_-]+(.db|.cql|.json|.crc32|TOC.txt)");
    private static final Set<String> FORBIDDEN_DIRS = new HashSet<>(
            Arrays.asList("system_schema", "system_traces", "system_distributed", "system", "system_auth"));

    private final InstanceMetadataFetcher metadataFetcher;
    private final FileStreamer fileStreamer;

    @Inject
    public StreamSSTableComponent(final InstanceMetadataFetcher metadataFetcher, final FileStreamer fileStreamer)
    {
        this.metadataFetcher = metadataFetcher;
        this.fileStreamer = fileStreamer;
    }

    @GET
    @javax.ws.rs.Path("/v1/stream/keyspace/{keyspace}/table/{table}/snapshot/{snapshot}/component/{component}")
    public void streamFromFirstInstance(@PathParam("keyspace") String keyspace, @PathParam("table") String table,
                                        @PathParam("snapshot") String snapshot,
                                        @PathParam("component") String component, @HeaderParam("Range") String range,
                                        @Context HttpServerResponse resp, @Context HttpServerRequest req)
    {
        final String host = req.host().contains(":")
                            ? req.host().substring(0, req.host().indexOf(":"))
                            : req.host();
        stream(new ComponentInfo(keyspace, table, snapshot, component), range, null, host, resp);
    }

    @GET
    @javax.ws.rs.Path
    ("/v1/stream/instance/{instanceId}/keyspace/{keyspace}/table/{table}/snapshot/{snapshot}/component/{component}")
    public void streamFromSpecificInstance(@PathParam("keyspace") String keyspace, @PathParam("table") String table,
                                           @PathParam("snapshot") String snapshot,
                                           @PathParam("component") String component,
                                           @PathParam("instanceId") Integer instanceId,
                                           @HeaderParam("Range") String range, @Context HttpServerResponse resp)
    {
        stream(new ComponentInfo(keyspace, table, snapshot, component), range, instanceId, null, resp);
    }

    private void stream(ComponentInfo componentInfo, String range, Integer instanceId,
                        String host, HttpServerResponse resp)
    {
        final HttpResponse response = new HttpResponse(resp);
        if (FORBIDDEN_DIRS.contains(componentInfo.getKeyspace()))
        {
            response.setForbiddenStatus(componentInfo.getKeyspace() + " keyspace is forbidden");
            return;
        }
        if (!arePathParamsValid(componentInfo))
        {
            response.setBadRequestStatus("Invalid path params found");
            return;
        }

        final Path path;
        final FilePathBuilder pathBuilder = instanceId == null
                                            ? metadataFetcher.getPathBuilder(host)
                                            : metadataFetcher.getPathBuilder(instanceId);
        try
        {
            path = pathBuilder.build(componentInfo.getKeyspace(), componentInfo.getTable(),
                                     componentInfo.getSnapshot(), componentInfo.getComponent());
        }
        catch (FileNotFoundException e)
        {
            response.setNotFoundStatus(e.getMessage());
            return;
        }
        final File file = path.toFile();
        final Range r;
        try
        {
            r = parseRangeHeader(range, file.length());
        }
        catch (Exception e)
        {
            response.setRangeNotSatisfiable(e.getMessage());
            return;
        }
        fileStreamer.stream(response, file, r);
    }

    private boolean arePathParamsValid(ComponentInfo componentInfo)
    {
        return REGEX_DIR.matcher(componentInfo.getKeyspace()).matches()
               && REGEX_DIR.matcher(componentInfo.getTable()).matches()
               && REGEX_DIR.matcher(componentInfo.getSnapshot()).matches()
               && REGEX_COMPONENT.matcher(componentInfo.getComponent()).matches();
    }

    private Range parseRangeHeader(String rangeHeader, long fileSize)
    {
        final Range fileRange = new Range(0, fileSize - 1, fileSize);
        // sidecar does not support multiple ranges as of now
        final Range headerRange = Range.parseHeader(rangeHeader, fileSize);
        return fileRange.intersect(headerRange);
    }
}

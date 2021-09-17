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
import io.vertx.core.http.HttpServerResponse;
import org.apache.cassandra.sidecar.models.HttpResponse;
import org.apache.cassandra.sidecar.models.Range;
import org.apache.cassandra.sidecar.utils.FilePathBuilder;
import org.apache.cassandra.sidecar.utils.FileStreamer;

/**
 * Handler for serving SSTable components from snapshot folders
 */
@Singleton
@javax.ws.rs.Path("/api/v1/stream/keyspace/{keyspace}/table/{table}/snapshot/{snapshot}/component/{component}")
public class StreamSSTableComponent
{
    private static final Pattern REGEX_DIR = Pattern.compile("[a-zA-Z0-9_-]+");
    private static final Pattern REGEX_COMPONENT = Pattern.compile("[a-zA-Z0-9_-]+(.db|.cql|.json|.crc32|TOC.txt)");
    private static final Set<String> FORBIDDEN_DIRS = new HashSet<>(
            Arrays.asList("system_schema", "system_traces", "system_distributed", "system", "system_auth"));

    private final FilePathBuilder pathBuilder;
    private final FileStreamer fileStreamer;

    @Inject
    public StreamSSTableComponent(final FilePathBuilder pathBuilder, final FileStreamer fileStreamer)
    {
        this.pathBuilder = pathBuilder;
        this.fileStreamer = fileStreamer;
    }

    @GET
    public void handle(@PathParam("keyspace") String keyspace, @PathParam("table") String table,
                       @PathParam("snapshot") String snapshot, @PathParam("component") String component,
                       @HeaderParam("Range") String range, @Context HttpServerResponse resp)
    {
        final HttpResponse response = new HttpResponse(resp);
        if (FORBIDDEN_DIRS.contains(keyspace))
        {
            response.setForbiddenStatus(keyspace + " keyspace is forbidden");
            return;
        }
        if (!arePathParamsValid(keyspace, table, snapshot, component))
        {
            response.setBadRequestStatus("Invalid path params found");
            return;
        }

        final Path path;
        try
        {
            path = pathBuilder.build(keyspace, table, snapshot, component);
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

    private boolean arePathParamsValid(String keyspace, String table, String snapshot, String component)
    {
        return REGEX_DIR.matcher(keyspace).matches() && REGEX_DIR.matcher(table).matches()
                && REGEX_DIR.matcher(snapshot).matches() && REGEX_COMPONENT.matcher(component).matches();
    }

    private Range parseRangeHeader(String rangeHeader, long fileSize)
    {
        final Range fileRange = new Range(0, fileSize - 1, fileSize);
        // sidecar does not support multiple ranges as of now
        final Range headerRange = Range.parseHeader(rangeHeader, fileSize);
        return fileRange.intersect(headerRange);
    }
}

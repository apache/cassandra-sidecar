package org.apache.cassandra.sidecar.routes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.file.FileProps;
import io.vertx.core.file.FileSystem;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.models.HttpResponse;
import org.apache.cassandra.sidecar.utils.FileStreamer;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE;
import static org.apache.cassandra.sidecar.utils.RequestUtils.extractHostAddressWithoutPort;

/**
 * Handler for sending out files.
 */
public class FileStreamHandler implements Handler<RoutingContext>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileStreamHandler.class);
    public static final String FILE_PATH_CONTEXT_KEY = "fileToTransfer";

    private final FileStreamer fileStreamer;

    @Inject
    public FileStreamHandler(final FileStreamer fileStreamer)
    {
        this.fileStreamer = fileStreamer;
    }

    @Override
    public void handle(RoutingContext context)
    {
        final String localFile = context.get(FILE_PATH_CONTEXT_KEY);
        final HttpServerRequest request = context.request();
        final String host = extractHostAddressWithoutPort(request.host());

        LOGGER.debug("FileStreamHandler handle file transfer '{}' for client: {}. Instance: {}", localFile,
                     request.remoteAddress(), host);

        FileSystem fs = context.vertx().fileSystem();
        fs.exists(localFile)
          .compose(exists -> ensureValidFile(fs, localFile, exists))
          .compose(fileProps -> fileStreamer.stream(new HttpResponse(request, context.response()), localFile,
                                                    fileProps.size(), request.getHeader(HttpHeaderNames.RANGE)))
          .onSuccess(v -> LOGGER.debug("Completed streaming file '{}'", localFile))
          .onFailure(context::fail);
    }

    /**
     * Ensures that the file exists and is a non-empty regular file
     *
     * @param fs        The underlying filesystem
     * @param localFile The path the file in the filesystem
     * @param exists    Whether the file exists or not
     * @return a succeeded future with the {@link FileProps}, or a failed future if the file does not exist;
     * is not a regular file; or if the file is empty
     */
    private Future<FileProps> ensureValidFile(FileSystem fs, String localFile, Boolean exists)
    {
        if (!exists)
        {
            LOGGER.error("The requested file '{}' does not exist", localFile);
            return Future.failedFuture(new HttpException(NOT_FOUND.code(), "The requested file does not exist"));
        }

        return fs.props(localFile)
                 .compose(fileProps ->
                  {
                     if (fileProps == null || !fileProps.isRegularFile())
                     {
                         // File is not a regular file
                         LOGGER.error("The requested file '{}' does not exist", localFile);
                         return Future.failedFuture(new HttpException(NOT_FOUND.code(),
                                                                      "The requested file does not exist"));
                     }

                     if (fileProps.size() <= 0)
                     {
                         LOGGER.error("The requested file '{}' has 0 size", localFile);
                         return Future.failedFuture(new HttpException(REQUESTED_RANGE_NOT_SATISFIABLE.code(),
                                                                      "The requested file is empty"));
                     }

                     return Future.succeededFuture(fileProps);
                 });
    }
}

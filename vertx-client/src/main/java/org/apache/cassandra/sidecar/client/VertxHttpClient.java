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

package org.apache.cassandra.sidecar.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.net.KeyCertOptions;
import io.vertx.core.net.KeyStoreOptions;
import io.vertx.core.net.OpenSSLEngineOptions;
import io.vertx.core.net.TrustOptions;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.client.predicate.ResponsePredicateResult;
import io.vertx.ext.web.codec.BodyCodec;
import org.apache.cassandra.sidecar.common.request.Request;
import org.apache.cassandra.sidecar.common.request.UploadableRequest;

/**
 * An {@link HttpClient} implementation that uses vertx's WebClient internally
 */
public class VertxHttpClient implements HttpClient
{
    private static final Logger LOGGER = LoggerFactory.getLogger(VertxHttpClient.class);

    protected final Vertx vertx;
    protected final WebClient webClient;
    protected final HttpClientConfig config;

    public VertxHttpClient(Vertx vertx, HttpClientConfig config)
    {
        WebClientOptions options = new WebClientOptions()
                                   .setMaxPoolSize(config.maxPoolSize())
                                   .setIdleTimeout(config.idleTimeoutMillis())
                                   .setIdleTimeoutUnit(TimeUnit.MILLISECONDS)
                                   .setMaxChunkSize(config.maxChunkSize())
                                   .setReceiveBufferSize(config.receiveBufferSize())
                                   .setConnectTimeout((int) config.timeoutMillis())
                                   .setUserAgent(config.userAgent());

        options = applySSLOptions(options, config);

        this.vertx = vertx;
        this.webClient = WebClient.create(vertx, options);
        this.config = config;
    }

    public VertxHttpClient(Vertx vertx, WebClient webClient, HttpClientConfig config)
    {
        this.vertx = vertx;
        this.webClient = webClient;
        this.config = config;
    }

    Vertx vertx()
    {
        return vertx;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HttpClientConfig config()
    {
        return config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<HttpResponse> execute(SidecarInstance sidecarInstance, RequestContext context)
    {
        if (context.request() instanceof UploadableRequest)
        {
            HttpRequest<Buffer> vertxRequest = vertxRequest(sidecarInstance, context);
            UploadableRequest uploadableRequest = (UploadableRequest) context.request();
            LOGGER.debug("Uploading file={}, for request={}, instance={}",
                         uploadableRequest.filename(), context.request(), sidecarInstance);
            return executeUploadFileInternal(sidecarInstance, vertxRequest, uploadableRequest.filename());
        }
        else
        {
            LOGGER.debug("Executing request={}, on instance={}", context.request(), sidecarInstance);
            return executeInternal(sidecarInstance, context);
        }
    }

    protected CompletableFuture<HttpResponse> executeInternal(SidecarInstance sidecarInstance, RequestContext context)
    {
        Future<HttpRequest<Buffer>> future = Future.future(promise -> promise.complete(vertxRequest(sidecarInstance, context)
                                                                                       .ssl(config.ssl())
                                                                                       .timeout(config.timeoutMillis())));

        return future
               .compose(vertxRequest -> {
                   Request request = context.request();
                   if (request.requestBody() != null)
                   {
                       return vertxRequest.sendJson(request.requestBody());
                   }
                   return vertxRequest.send();
               })
               .map(response -> {
                   byte[] raw = response.body() != null ? response.body().getBytes() : null;
                   return (HttpResponse) new HttpResponseImpl(response.statusCode(),
                                                              response.statusMessage(),
                                                              raw,
                                                              mapHeaders(response.headers()),
                                                              sidecarInstance
                   );
               })
               .toCompletionStage().toCompletableFuture();
    }

    protected CompletableFuture<HttpResponse> executeUploadFileInternal(SidecarInstance sidecarInstance,
                                                                        HttpRequest<Buffer> vertxRequest,
                                                                        String filename)
    {
        Promise<HttpResponse> promise = Promise.promise();
        // open the local file
        openFileForRead(vertx.fileSystem(), filename)
        .compose(pair -> vertxRequest.ssl(config.ssl())
                                     .putHeader(HttpHeaderNames.CONTENT_LENGTH.toString(),
                                                String.valueOf(pair.getKey()))
                                     .sendStream(pair.getValue()
                                                     .setReadBufferSize(config.sendReadBufferSize())))
        .onFailure(promise::fail)
        .onSuccess(response -> {
            byte[] raw = response.body() != null ? response.body().getBytes() : null;
            promise.complete(new HttpResponseImpl(response.statusCode(),
                                                  response.statusMessage(),
                                                  raw,
                                                  mapHeaders(response.headers()),
                                                  sidecarInstance
            ));
        });

        return promise.future().toCompletionStage().toCompletableFuture();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<HttpResponse> stream(SidecarInstance sidecarInstance,
                                                  RequestContext context,
                                                  StreamConsumer streamConsumer)
    {
        Objects.requireNonNull(streamConsumer, "The streamConsumer must be set");
        HttpRequest<Buffer> vertxRequest = vertxRequest(sidecarInstance, context);

        LOGGER.debug("Streaming request={}, from instance={}", context.request(), sidecarInstance);

        Promise<HttpResponse> promise = Promise.promise();
        vertxRequest.ssl(config.ssl())
                    .timeout(config.timeoutMillis())
                    .expect(response -> {

                        // fulfill the promise with the response
                        promise.complete(new HttpResponseImpl(response.statusCode(),
                                                              response.statusMessage(),
                                                              mapHeaders(response.headers()),
                                                              sidecarInstance));

                        if (response.statusCode() == HttpResponseStatus.OK.code() ||
                            response.statusCode() == HttpResponseStatus.PARTIAL_CONTENT.code())
                        {
                            return ResponsePredicateResult.success();
                        }
                        else
                        {
                            LOGGER.warn("Unexpected status code received statusCode={}, statusMessage={}",
                                        response.statusCode(), response.statusMessage());
                            return ResponsePredicateResult.failure("Unexpected status code: " +
                                                                   response.statusCode());
                        }
                    })
                    .as(BodyCodec.pipe(new StreamConsumerWriteStream(streamConsumer)))
                    .send()
                    .onFailure(throwable -> {
                        if (!promise.tryFail(throwable))
                        {
                            // the stream has already started, we need to signal the consumer that the
                            // there was a failure mid-stream. This is a non-retryable case
                            streamConsumer.onError(throwable);
                        }
                    });
        return promise.future().toCompletionStage().toCompletableFuture();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close()
    {
        webClient.close();
    }

    protected HttpRequest<Buffer> vertxRequest(SidecarInstance sidecarInstance, RequestContext context)
    {
        Request request = context.request();
        HttpMethod method = HttpMethod.valueOf(request.method().name());
        HttpRequest<Buffer> vertxRequest = webClient.request(method,
                                                             sidecarInstance.port(),
                                                             sidecarInstance.hostname(),
                                                             request.requestURI());

        vertxRequest = applyHeaders(vertxRequest, request.headers());

        Map<String, String> customHeaders = context.customHeaders();
        if (customHeaders != null && !customHeaders.isEmpty())
        {
            vertxRequest = applyHeaders(vertxRequest, customHeaders);
        }

        return vertxRequest;
    }

    protected HttpRequest<Buffer> applyHeaders(HttpRequest<Buffer> vertxRequest, Map<String, String> headers)
    {
        if (headers == null || headers.isEmpty())
            return vertxRequest;

        for (Map.Entry<String, String> header : headers.entrySet())
        {
            vertxRequest = vertxRequest.putHeader(header.getKey(), header.getValue());
        }
        return vertxRequest;
    }

    protected Map<String, List<String>> mapHeaders(MultiMap headers)
    {
        if (headers == null)
        {
            return Collections.emptyMap();
        }
        return headers.entries().stream()
                      .filter(entry -> entry.getKey() != null && entry.getValue() != null)
                      .collect(Collectors.toMap(Map.Entry::getKey,
                                                entry -> Collections.singletonList(entry.getValue())));
    }

    protected static WebClientOptions applySSLOptions(WebClientOptions options, HttpClientConfig config)
    {
        if (!config.ssl())
        {
            return options;
        }

        options = options.setSsl(true);

        if (config.trustStoreInputStream() != null && config.trustStorePassword() != null)
        {
            TrustOptions trustOptions = buildKeyCertOptions(config.trustStoreInputStream(),
                                                            config.trustStorePassword(),
                                                            config.trustStoreType());
            options = options.setTrustOptions(trustOptions);
        }

        if (config.keyStoreInputStream() != null && config.keyStorePassword() != null)
        {
            KeyCertOptions keyCertOptions = buildKeyCertOptions(config.keyStoreInputStream(),
                                                                config.keyStorePassword(),
                                                                config.keyStoreType());
            options = options.setKeyCertOptions(keyCertOptions);
        }

        if (OpenSSLEngineOptions.isAvailable())
        {
            LOGGER.info("Building Sidecar vertx client with OpenSSL");
            options = options.setOpenSslEngineOptions(new OpenSSLEngineOptions());
        }
        else
        {
            LOGGER.warn("OpenSSL not available when building Sidecar vertx client");
        }

        return options;
    }

    protected static KeyStoreOptions buildKeyCertOptions(InputStream storeStream, String storePass, String storeType)
    {
        try (InputStream inputStream = storeStream)
        {
            byte[] trustBytes = readStore(inputStream);
            return new KeyStoreOptions().setType(storeType)
                                        .setPassword(storePass)
                                        .setValue(Buffer.buffer(trustBytes));
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to load default truststore.", e);
        }
    }

    protected static byte[] readStore(InputStream storeStream) throws IOException
    {
        byte[] buffer = new byte[1024];
        int temp;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        while ((temp = storeStream.read(buffer)) != -1)
        {
            bos.write(buffer, 0, temp);
        }
        return bos.toByteArray();
    }

    protected Future<AbstractMap.SimpleEntry<Long, AsyncFile>> openFileForRead(FileSystem fs, String filename)
    {
        Promise<AbstractMap.SimpleEntry<Long, AsyncFile>> promise = Promise.promise();
        fs.exists(filename)
          .compose(exists -> {
              if (!exists)
              {
                  String errMsg = "File '" + filename + "' does not exist";
                  return Future.failedFuture(new NoSuchFileException(errMsg));
              }
              return fs.props(filename);
          })
          .onSuccess(props -> fs.open(filename, new OpenOptions().setWrite(false).setCreate(false).setRead(true))
                                .onFailure(promise::tryFail)
                                .onSuccess(asyncFile -> promise.complete(new AbstractMap.SimpleEntry<>(props.size(),
                                                                                                       asyncFile))))
          .onFailure(promise::tryFail);
        return promise.future();
    }
}

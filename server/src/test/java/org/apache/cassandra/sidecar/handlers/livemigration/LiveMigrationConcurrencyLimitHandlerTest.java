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

package org.apache.cassandra.sidecar.handlers.livemigration;

import org.junit.jupiter.api.Test;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.HelperTestModules.RoutingContextTestModule;
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LiveMigrationConcurrencyLimitHandlerTest
{
    private Injector createInjector(int maxConcurrentFileRequests)
    {
        return Guice.createInjector(new TestModule(maxConcurrentFileRequests));
    }

    @Test
    public void testPermitAcquiredWhenUnderLimit()
    {
        Injector injector = createInjector(2);
        LiveMigrationConcurrencyLimitHandler handler = injector.getInstance(LiveMigrationConcurrencyLimitHandler.class);
        RoutingContext rc = injector.getInstance(RoutingContext.class);

        handler.handle(rc);

        verify(rc, times(1)).next();
        verify(rc, times(1)).addEndHandler(any());
        verify(rc, never()).fail(any(Throwable.class));
    }

    @Test
    public void testRequestRejectedWhenLimitExceeded()
    {
        Injector injector = createInjector(1);
        LiveMigrationConcurrencyLimitHandler handler = injector.getInstance(LiveMigrationConcurrencyLimitHandler.class);
        RoutingContext rc = injector.getInstance(RoutingContext.class);

        // First request acquires the only available permit
        handler.handle(rc);
        verify(rc, times(1)).next();

        // Second request exceeds the limit and should be rejected
        handler.handle(rc);
        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(rc, times(1)).fail(captor.capture());
        assertThat(captor.getValue()).isInstanceOf(HttpException.class);
        assertThat(((HttpException) captor.getValue()).getStatusCode()).isEqualTo(HttpResponseStatus.SERVICE_UNAVAILABLE.code());
        // next() was only called for the first request
        verify(rc, times(1)).next();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPermitReleasedAfterEndHandler()
    {
        Injector injector = createInjector(1);
        LiveMigrationConcurrencyLimitHandler handler = injector.getInstance(LiveMigrationConcurrencyLimitHandler.class);
        RoutingContext rc = injector.getInstance(RoutingContext.class);

        // Acquire the single permit
        handler.handle(rc);
        verify(rc, times(1)).next();

        // Capture the registered end handler and fire it to simulate request completion
        ArgumentCaptor<Handler<AsyncResult<Void>>> endHandlerCaptor = ArgumentCaptor.forClass(Handler.class);
        verify(rc, times(1)).addEndHandler(endHandlerCaptor.capture());
        endHandlerCaptor.getValue().handle(null);

        // Permit was released; the next request should succeed
        handler.handle(rc);
        verify(rc, times(2)).next();
        verify(rc, never()).fail(any(Throwable.class));
    }

    @Test
    public void testAllPermitsFilledThenRejected()
    {
        int limit = 3;
        Injector injector = createInjector(limit);
        LiveMigrationConcurrencyLimitHandler handler = injector.getInstance(LiveMigrationConcurrencyLimitHandler.class);
        RoutingContext rc = injector.getInstance(RoutingContext.class);

        // Fill all permits up to the limit - each should succeed
        for (int i = 0; i < limit; i++)
        {
            handler.handle(rc);
        }
        verify(rc, times(limit)).next();

        // One more request beyond the limit should be rejected
        handler.handle(rc);
        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(rc, times(1)).fail(captor.capture());
        assertThat(captor.getValue()).isInstanceOf(HttpException.class);
        assertThat(((HttpException) captor.getValue()).getStatusCode()).isEqualTo(HttpResponseStatus.SERVICE_UNAVAILABLE.code());
    }

    private static class TestModule extends AbstractModule
    {
        private final int maxConcurrentFileRequests;

        TestModule(int maxConcurrentFileRequests)
        {
            this.maxConcurrentFileRequests = maxConcurrentFileRequests;
        }

        @Override
        protected void configure()
        {
            LiveMigrationConfiguration liveMigrationConfig = mock(LiveMigrationConfiguration.class);
            when(liveMigrationConfig.maxConcurrentFileRequests()).thenReturn(maxConcurrentFileRequests);

            SidecarConfiguration sidecarConfiguration = mock(SidecarConfiguration.class);
            when(sidecarConfiguration.liveMigrationConfiguration()).thenReturn(liveMigrationConfig);

            bind(SidecarConfiguration.class).toInstance(sidecarConfiguration);
            install(new RoutingContextTestModule());
        }
    }
}

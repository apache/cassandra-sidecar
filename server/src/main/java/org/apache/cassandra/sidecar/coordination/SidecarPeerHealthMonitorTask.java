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

package org.apache.cassandra.sidecar.coordination;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.codecs.SidecarInstanceCodec;
import org.apache.cassandra.sidecar.common.client.SidecarInstance;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.SidecarPeerHealthConfiguration;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.tasks.ScheduleDecision;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SIDECAR_PEER_DOWN;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SIDECAR_PEER_UP;

/**
 * Pings other 'peer' Sidecar(s) that are relevant to this Sidecar over HTTP and notifies
 * listeners when other Sidecar(s) goes DOWN or OK.
 */
@Singleton
public class SidecarPeerHealthMonitorTask implements PeriodicTask
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarPeerHealthMonitorTask.class);

    private final Vertx vertx;
    private final SidecarPeerHealthConfiguration config;
    private final SidecarPeerProvider sidecarPeerProvider;
    private final SidecarPeerHealthProvider healthProvider;

    private final Map<SidecarInstance, SidecarPeerHealthProvider.Health> status = new ConcurrentHashMap<>();

    @Inject
    public SidecarPeerHealthMonitorTask(Vertx vertx,
                                        SidecarConfiguration sidecarConfiguration,
                                        SidecarPeerProvider sidecarPeerProvider,
                                        SidecarPeerHealthProvider healthProvider,
                                        SidecarInstanceCodec sidecarInstanceCodec)
    {
        this.vertx = vertx;
        this.config = sidecarConfiguration.sidecarPeerHealthConfiguration();
        this.sidecarPeerProvider = sidecarPeerProvider;
        this.healthProvider = healthProvider;
        // TODO: Find a better place to register this codec
        vertx.eventBus().registerDefaultCodec(SidecarInstance.class, sidecarInstanceCodec);
    }

    @Override
    public ScheduleDecision scheduleDecision()
    {
        return config.enabled() ? ScheduleDecision.EXECUTE : ScheduleDecision.SKIP;
    }

    @Override
    public DurationSpec delay()
    {
        return config.executeInterval();
    }

    @Override
    public void execute(Promise<Void> promise)
    {
        try
        {
            run().onSuccess(v -> promise.tryComplete())
                 .onFailure(promise::tryFail);
        }
        catch (Throwable t)
        {
            LOGGER.error("Unexpected error running down detector", t);
            promise.fail(t);
        }
    }

    // internal methods
    protected Future<CompositeFuture> run()
    {
        Set<SidecarInstance> sidecarPeers = sidecarPeerProvider.get();
        if (sidecarPeers.isEmpty())
        {
            LOGGER.warn("No Sidecar sidecarPeers detected");
            return Future.succeededFuture();
        }

        List<Future<SidecarPeerHealthProvider.Health>> futures =
        sidecarPeers.stream()
                    .map(instance ->
                         healthProvider.health(instance)
                                       .andThen(ar -> {
                                           if (ar.succeeded())
                                           {
                                               updateHealth(instance, ar.result());
                                           }
                                           else
                                           {
                                               LOGGER.error("Failed to run health check, marking instance as DOWN host={} port={}",
                                                            instance.hostname(), instance.port(), ar.cause());
                                               markDown(instance);
                                           }
                                       }))
                    .collect(Collectors.toList());

        return Future.all(futures)
                     .onComplete(f -> {
                         if (f.succeeded())
                         {
                             status.keySet().retainAll(sidecarPeers);
                         }
                         else
                         {
                             LOGGER.error("Unexpected error in down detector", f.cause());
                         }
                     });
    }

    // listener notifications
    protected void updateHealth(SidecarInstance instance, SidecarPeerHealthProvider.Health health)
    {
        switch (health)
        {
            case UP:
                markOk(instance);
                break;
            case DOWN:
                markDown(instance);
                break;
        }
    }

    protected void markOk(SidecarInstance instance)
    {
        if (compareAndUpdate(instance, SidecarPeerHealthProvider.Health.UP))
        {
            LOGGER.info("Sidecar instance is now OK hostname={} port={}", instance.hostname(), instance.port());
            vertx.eventBus().publish(ON_SIDECAR_PEER_UP.address(), instance);
        }
    }

    protected void markDown(SidecarInstance instance)
    {
        if (compareAndUpdate(instance, SidecarPeerHealthProvider.Health.DOWN))
        {
            LOGGER.warn("Sidecar instance is now DOWN hostname={} port={}", instance.hostname(), instance.port());
            vertx.eventBus().publish(ON_SIDECAR_PEER_DOWN.address(), instance);
        }
    }

    protected boolean compareAndUpdate(SidecarInstance instance, SidecarPeerHealthProvider.Health newStatus)
    {
        return status.put(instance, newStatus) != newStatus;
    }

    public Map<SidecarInstance, SidecarPeerHealthProvider.Health> getStatus()
    {
        return status;
    }
}

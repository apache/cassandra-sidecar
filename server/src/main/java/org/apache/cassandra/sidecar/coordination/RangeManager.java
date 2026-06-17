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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.codecs.RangeChangeEventCodec;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.spark.utils.Pair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CASSANDRA_CQL_READY;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SIDECAR_PEER_DOWN;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SIDECAR_PEER_UP;
import static org.apache.cassandra.sidecar.tasks.ClusterTopologyMonitor.ClusterTopologyEventType.ON_DC_TOPOLOGY_CHANGE;

/**
 * This class manages the token ranges owned by this Sidecar instance and listens on the DownDetector for other Sidecar instances going up/down.
 * The underlying implementation can implement a consensus algorithm to provide strong guarantees around gaining/releasing token range ownership.
 */
public abstract class RangeManager implements Handler<Message<Object>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RangeManager.class);

    /**
     * Events published when token range ownership changes.
     */
    public enum LeadershipEvents
    {
        ON_TOKEN_RANGE_GAINED,
        ON_TOKEN_RANGE_LOST;

        /**
         * Returns the event bus address for this leadership event type.
         */
        public String address()
        {
            return LeadershipEvents.class.getName() + "." + name();
        }
    }

    /**
     * Events published when the token ring topology changes.
     */
    public enum RangeManagerEvents
    {
        ON_TOKEN_RANGE_CHANGED;

        /**
         * Returns the event bus address for this range manager event type.
         */
        public String address()
        {
            return RangeManagerEvents.class.getName() + "." + name();
        }
    }

    /**
     * Event data containing information about token range ownership changes.
     */
    public static class RangeChangeEvent
    {
        public final ImmutableMap<String, Set<TokenRange>> change; // ranges gained/lost by this change
        public final ImmutableMap<String, Set<TokenRange>> newView; // new view of the ring after this change

        /**
         * Creates a new range change event with the specified changes and new view.
         */
        public RangeChangeEvent(Map<String, Set<TokenRange>> change, Map<String, Set<TokenRange>> newView)
        {
            this.change = ImmutableMap.copyOf(change);
            this.newView = ImmutableMap.copyOf(newView);
        }

        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            RangeChangeEvent event = (RangeChangeEvent) o;
            return Objects.equals(change, event.change) && Objects.equals(newView, event.newView);
        }

        public int hashCode()
        {
            return Objects.hash(change, newView);
        }
    }

    protected final Vertx vertx;
    protected final TokenRingProvider tokenRingProvider;
    @Nullable
    protected volatile ImmutableMap<String, Set<TokenRange>> ownedRanges = null;

    public RangeManager(Vertx vertx, TokenRingProvider tokenRingProvider)
    {
        this.vertx = vertx;
        this.tokenRingProvider = tokenRingProvider;
        initializeRanges();
        try
        {
            vertx.eventBus().registerDefaultCodec(RangeChangeEvent.class, RangeChangeEventCodec.INSTANCE);
        }
        catch (IllegalStateException ise)
        {
            if (!ise.getMessage().contains("Already a default codec registered"))
            {
                throw ise;
            }
        }
        vertx.eventBus().localConsumer(ON_CASSANDRA_CQL_READY.address(), this);
        vertx.eventBus().localConsumer(ON_SIDECAR_PEER_UP.address(), this);
        vertx.eventBus().localConsumer(ON_SIDECAR_PEER_DOWN.address(), this);
        vertx.eventBus().localConsumer(ON_DC_TOPOLOGY_CHANGE.address(), this);
    }

    /**
     * Returns the token ranges currently owned by this Sidecar instance.
     */
    @Nullable
    public Map<String, Set<TokenRange>> ownedTokenRanges()
    {
        return this.ownedRanges;
    }

    // Sidecar coordination

    /**
     * @param primaryOwner SidecarInstance that is the primary owner for the token ranges.
     * @param ranges       token ranges this SidecarInstance wishes to gain ownership.
     * @return a future that completes when propose request completes, returning the ranges that were successfully gained as part of the request.
     */
    /**
     * Proposes to gain ownership of the specified token ranges.
     */
    abstract Future<Boolean> proposeOwnership(SidecarInstance primaryOwner, Map<String, Set<TokenRange>> ranges);

    /**
     * @param primaryOwner SidecarInstance that is the primary owner for the token ranges.
     * @param ranges       token ranges this SidecarInstance wishes to release.
     * @return a future that completes when release request completes, returning the ranges that were released as part of the request.
     */
    /**
     * Proposes to release ownership of the specified token ranges.
     */
    abstract Future<Boolean> releaseOwnership(SidecarInstance primaryOwner, Map<String, Set<TokenRange>> ranges);

    // DownDetector notification handler

    /**
     * Handles event bus messages for peer up/down and topology changes.
     */
    @Override
    public void handle(Message<Object> msg)
    {
        if (ON_SIDECAR_PEER_UP.address().equals(msg.address()))
        {
            onSidecarUp((SidecarInstance) msg.body());
        }
        else if (ON_SIDECAR_PEER_DOWN.address().equals(msg.address()))
        {
            onSidecarDown((SidecarInstance) msg.body());
        }
        else if (ON_CASSANDRA_CQL_READY.address().equals(msg.address()) || ON_DC_TOPOLOGY_CHANGE.address().equals(msg.address()))
        {
            initializeRanges();
        }
    }

    /**
     * Initializes owned token ranges from the token ring provider.
     */
    protected synchronized void initializeRanges()
    {
        LOGGER.info("Initializing ranges");
        final String dc = getLocalDcSafe();
        if (dc == null)
        {
            LOGGER.error("DC null, cannot initialize owned ranges");
            return;
        }
        ImmutableMap<String, Set<TokenRange>> newRanges = ImmutableMap.copyOf(toSet(tokenRingProvider.getPrimaryTokenRanges(dc)));
        if (!newRanges.equals(this.ownedRanges))
        {
            this.ownedRanges = newRanges;
            vertx.eventBus().publish(RangeManagerEvents.ON_TOKEN_RANGE_CHANGED.address(), null);
        }
    }

    /**
     * Handles a Sidecar instance coming online by releasing its primary ranges.
     */
    protected synchronized void onSidecarUp(SidecarInstance instance)
    {
        // on Sidecar detected to be UP, initiate proposal to release ownership of its primary ranges
        final String dc = getLocalDcSafe();
        if (dc == null)
        {
            LOGGER.error("DC null, cannot respond to onSidecarUp event");
            return;
        }
        Map<String, Set<TokenRange>> primaryRanges = toSet(tokenRingProvider.getPrimaryRanges(instance, dc));
        if (primaryRanges.isEmpty())
        {
            LOGGER.warn("Empty primary token ranges for Sidecar host={} port={}", instance.hostname(), instance.port());
            return;
        }

        releaseOwnership(instance, primaryRanges)
        .onSuccess(result -> {
            if (result)
            {
                onRangesLost(primaryRanges);
            }
            else
            {
                LOGGER.warn("Failed to release ownership of instance primary range host={} port={} ranges='{}'",
                            instance.hostname(), instance.port(), primaryRanges);
            }
        })
        .onFailure(throwable -> LOGGER.warn("Error attempting to release range ownership", throwable));
    }

    /**
     * Handles a Sidecar instance going offline by gaining its primary ranges.
     */
    protected synchronized void onSidecarDown(SidecarInstance instance)
    {
        // on Sidecar detected to be DOWN, initiate proposal to gain ownership of its primary ranges
        final String dc = getLocalDcSafe();
        if (dc == null)
        {
            LOGGER.error("DC null, cannot respond to onSidecarDown event");
            return;
        }
        Map<String, Set<TokenRange>> primaryRanges = toSet(tokenRingProvider.getPrimaryRanges(instance, dc));
        if (primaryRanges.isEmpty())
        {
            LOGGER.warn("Empty primary token ranges for Sidecar host={} port={}", instance.hostname(), instance.port());
            return;
        }

        proposeOwnership(instance, primaryRanges)
        .onSuccess(result -> {
            if (result)
            {
                onRangesGained(primaryRanges);
            }
            else
            {
                LOGGER.warn("Failed to gain ownership of instance primary range host={} port={} ranges='{}'",
                            instance.hostname(), instance.port(), primaryRanges);
            }
        })
        .onFailure(throwable -> LOGGER.warn("Error attempting to gain range ownership", throwable));
    }

    // Range change events

    /**
     * Handles gaining ownership of token ranges and publishes the change event.
     */
    public synchronized void onRangesGained(Map<String, Set<TokenRange>> gainedRanges)
    {
        ImmutableMap<String, Set<TokenRange>> ownedRanges = this.ownedRanges;
        if (ownedRanges == null)
        {
            LOGGER.warn("Cannot handle gained range notification, cql session not initialized yet ranges='{}'", gainedRanges);
            return;
        }
        ImmutableMap<String, Set<TokenRange>> newView = ImmutableMap.copyOf(unionRangeMap(ownedRanges, gainedRanges));
        if (!newView.equals(ownedRanges))
        {
            LOGGER.info("Gained ownership of ranges ranges='{}'", gainedRanges);
            this.ownedRanges = newView;
            this.vertx.eventBus().publish(LeadershipEvents.ON_TOKEN_RANGE_GAINED.address(), new RangeChangeEvent(gainedRanges, newView));
        }
    }

    /**
     * Handles losing ownership of token ranges and publishes the change event.
     */
    public synchronized void onRangesLost(Map<String, Set<TokenRange>> lostRanges)
    {
        ImmutableMap<String, Set<TokenRange>> ownedRanges = this.ownedRanges;
        if (ownedRanges == null)
        {
            LOGGER.warn("Cannot handle lost range notification, cql session not initialized yet ranges='{}'", lostRanges);
            return;
        }
        ImmutableMap<String, Set<TokenRange>> newView = ImmutableMap.copyOf(differenceRangeMap(ownedRanges, lostRanges));
        if (!newView.equals(ownedRanges))
        {
            LOGGER.info("Lost ownership of ranges ranges='{}'", lostRanges);
            this.ownedRanges = newView;
            this.vertx.eventBus().publish(LeadershipEvents.ON_TOKEN_RANGE_LOST.address(), new RangeChangeEvent(lostRanges, newView));
        }
    }

    // utils

    protected static Map<String, Set<TokenRange>> unionRangeMap(@NotNull Map<String, Set<TokenRange>> currRanges, Map<String, Set<TokenRange>> newRanges)
    {
        if (newRanges == null || newRanges.isEmpty())
        {
            return currRanges;
        }
        else if (currRanges.isEmpty())
        {
            return newRanges;
        }

        Map<String, Set<TokenRange>> merged = new HashMap<>(currRanges);
        for (Map.Entry<String, Set<TokenRange>> entry : newRanges.entrySet())
        {
            merged.put(entry.getKey(), unionRanges(merged.get(entry.getKey()), entry.getValue()));
        }
        return merged;
    }

    protected static Set<TokenRange> unionRanges(@Nullable Set<TokenRange> current, @NotNull Set<TokenRange> gained)
    {
        if (current == null || current.isEmpty())
        {
            return gained;
        }
        else if (gained.isEmpty())
        {
            return current;
        }

        return Sets.union(current, gained).immutableCopy();
    }

    protected static Set<TokenRange> differenceRangeMap(@NotNull Set<TokenRange> current, @Nullable Set<TokenRange> lost)
    {
        if (lost == null || lost.isEmpty() || current.isEmpty())
        {
            return current;
        }
        return Sets.difference(current, lost).immutableCopy();
    }

    protected static Map<String, Set<TokenRange>> differenceRangeMap(@NotNull Map<String, Set<TokenRange>> curr, Map<String, Set<TokenRange>> lost)
    {
        return curr.entrySet().stream()
                   .map(e -> Pair.of(e.getKey(), differenceRangeMap(e.getValue(), lost.get(e.getKey()))))
                   .filter(f -> !f.right.isEmpty())
                   .collect(Collectors.toMap(e -> e.left, e -> e.right));
    }

    protected static Map<String, Set<TokenRange>> toSet(Map<String, List<TokenRange>> ranges)
    {
        return ranges.entrySet().stream()
                     .collect(Collectors.toMap(Map.Entry::getKey, e -> new HashSet<>(e.getValue())));
    }

    /**
     * Returns the local datacenter name safely, handling exceptions.
     */
    public String getLocalDcSafe()
    {
        try
        {
            return tokenRingProvider.localDc();
        }
        catch (RuntimeException runtimeException)
        {
            LOGGER.warn("localDC is unavailable", runtimeException);
            // localDc throws when no local instances are found
            return null;
        }
    }
}

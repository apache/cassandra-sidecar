/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.db;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.ProvisionException;

import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.CdcStatesSchema;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.utils.ByteBufUtils;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.TokenSplitUtil;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.TableIdentifier;
import org.apache.cassandra.spark.utils.ThrowableUtils;
import org.jetbrains.annotations.NotNull;

/**
 * Database accessor for CDC (Change Data Capture) state management operations.
 * This class provides specialized database access functionality for managing CDC state persistence
 * and retrieval in Cassandra Sidecar. It extends {@link DatabaseAccessor} to provide CDC-specific
 * operations.
 */
@SuppressWarnings("resource")
public class CdcDatabaseAccessor extends DatabaseAccessor<CdcStatesSchema>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CdcDatabaseAccessor.class);
    private final Provider<TokenSplitUtil> tokenSplitUtilProvider;
    private volatile TokenSplitUtil tokenSplitUtil = null;
    private final InstanceMetadataFetcher instanceMetadataFetcher;

    @Inject
    public CdcDatabaseAccessor(SidecarSchema sidecarSchema,
                               CQLSessionProvider sessionProvider,
                               Provider<TokenSplitUtil> tokenSplitUtilProvider,
                               InstanceMetadataFetcher instanceMetadataFetcher)
    {
        super(sidecarSchema.tableSchema(CdcStatesSchema.class), sessionProvider);
        this.tokenSplitUtilProvider = tokenSplitUtilProvider;
        this.instanceMetadataFetcher = instanceMetadataFetcher;
    }

    protected TokenSplitUtil tokenSplitUtil()
    {
        // cql connection must be initialized before we can initialize TokenSplitUtil class
        if (this.tokenSplitUtil == null)
        {
            try
            {
                this.tokenSplitUtil = tokenSplitUtilProvider.get();
            }
            catch (ProvisionException e)
            {
                Throwable root = ThrowableUtils.rootCause(e);
                LOGGER.error("Unable to provision TokenSplitUtil with unexpected exception", root);
                throw new RuntimeException(root);
            }
        }
        return this.tokenSplitUtil;
    }

    @NotNull
    public String fullSchema()
    {
        return session().getCluster().getMetadata().exportSchemaAsString();
    }

    @NotNull
    public UUID getTableId(TableIdentifier id)
    {
        return session().getCluster().getMetadata().getKeyspace(id.keyspace()).getTable(id.table()).getId();
    }

    public List<ResultSetFuture> storeStateAsync(String jobId,
                                                 TokenRange range,
                                                 ByteBuffer buf,
                                                 long timestamp)
    {
        // write state into all overlapping splits
        int[] splits = tokenSplitUtil().findOverlappingSplitIds(partitioner(), range);
        LOGGER.debug("Inserting CDC state into splits lower={} upper={} splits='{}'", 
                     range.lowerEndpoint(), range.upperEndpoint(),
                     Arrays.stream(splits).mapToObj(Integer::toString).collect(Collectors.joining(",")));
        return Arrays.stream(splits)
                     .mapToObj(split -> session().executeAsync(tableSchema.insertState().bind(
                     jobId,
                     (short) split,
                     range.lowerEndpoint(),
                     range.upperEndpoint(),
                     buf,
                     timestamp
                     )))
                     .collect(Collectors.toList());
    }

    /**
     * @param jobId Cdc job id
     * @param range token range
     * @return all previously stored CDC state data that overlaps with given token range.
     */
    @VisibleForTesting
    @NotNull
    public Stream<byte[]> loadStateForRange(String jobId, TokenRange range)
    {
        // read state from multiple shards that could overlap with range
        int[] splits = tokenSplitUtil().findOverlappingSplitIds(partitioner(), range);
        LOGGER.info("Reading CDC state from splits lower={} upper={} splits='{}'", 
                    range.lowerEndpoint(), range.upperEndpoint(),
                    Arrays.stream(splits).mapToObj(Integer::toString).collect(Collectors.joining(",")));
        Stream<ResultSetFuture> futures = Arrays.stream(splits)
                                                .mapToObj(split -> selectCdcRange(jobId, split));
        Stream<Row> rows = await(futures);
        return rows.filter(row -> !row.isNull(0) && !row.isNull(1) && !row.isNull(2))
                   .filter(row -> TokenSplitUtil.overlaps(range, row.getVarint(0), row.getVarint(1)))
                   .map(row -> ByteBufUtils.getArray(row.getBytes(2)));
    }

    @NotNull
    public Partitioner partitioner()
    {
        String[] tokens = session().getCluster().getMetadata().getPartitioner().split("\\.");
        return Partitioner.valueOf(tokens[tokens.length - 1]);
    }

    public static Stream<Row> await(Stream<ResultSetFuture> futures)
    {
        return futures.flatMap(f -> {
            try
            {
                return f.get().all().stream();
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            catch (ExecutionException e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    ResultSetFuture selectCdcRange(String jobId, int split)
    {
        return session().executeAsync(tableSchema.select().bind(jobId, (short) split));
    }
}

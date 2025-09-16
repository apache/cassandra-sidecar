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

package org.apache.cassandra.sidecar.db;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.inject.Provider;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.CdcStatesSchema;
import org.apache.cassandra.sidecar.db.schema.TableHistorySchema;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.TokenSplitUtil;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.db.CdcDatabaseAccessor.await;
import static org.apache.cassandra.sidecar.utils.TokenSplitUtil.overlaps;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link CdcDatabaseAccessor}
 */
class CdcDatabaseAccessorTests
{
    @Test
    void testDataStoreBasic()
    {
        MockCdcStateV2 datastore = new MockCdcStateV2();
        String jobId = UUID.randomUUID().toString();
        ByteBuffer buf1 = ByteBuffer.wrap(new byte[]{ 'a', 'b', 'c' });
        ByteBuffer buf2 = ByteBuffer.wrap(new byte[]{ 'e', 'f', 'g' });
        assertThat(datastore.isEmpty()).isTrue();
        assertThat(datastore.select(jobId, 0)).isEmpty();
        datastore.insert(jobId, 0, BigInteger.ZERO, BigInteger.TEN, buf1);
        assertThat(datastore.isEmpty()).isFalse();
        assertByteBufferEquals(buf1, datastore.selectBuffers(jobId, 0).stream().findFirst().orElseThrow());

        assertThat(datastore.select(jobId, 0)).isNotEmpty();
        assertThat(datastore.select(jobId, 0)).isNotEmpty();
        assertThat(datastore.select(jobId, 1)).isEmpty();
        assertThat(datastore.select(jobId, 1)).isEmpty();
        datastore.insert(jobId, 0, BigInteger.ZERO, BigInteger.valueOf(100), buf2);
        assertThat(datastore.select(jobId, 0)).hasSize(2);
    }

    @ParameterizedTest
    @ValueSource(ints = { 3, 4, 8, 16, 32, 64, 128, 256, 512, 1024 })
    void testDataStore(int numNodes)
    {
        Partitioner partitioner = Partitioner.Murmur3Partitioner;
        MockCdcStateV2 datastore = new MockCdcStateV2();
        String jobId = UUID.randomUUID().toString();
        List<BigInteger> tokens = TokenSplitUtil.splitTokens(numNodes, partitioner);
        TokenSplitUtil tokenSplitUtil = new TokenSplitUtil(numNodes);
        assertThat(datastore.isEmpty()).isTrue();

        // write state and verify we can read back
        ByteBuffer[] buffers = new ByteBuffer[tokens.size()];
        for (int i = 0; i < tokens.size(); i++)
        {
            BigInteger lower = tokens.get(i);
            BigInteger upper = i == tokens.size() - 1 ? partitioner.maxToken() : tokens.get(i + 1);
            TokenRange range = TokenRange.openClosed(lower, upper);
            ByteBuffer buf = randomBytes(i);
            buffers[i] = buf;
            int[] splits = tokenSplitUtil.findOverlappingSplitIds(partitioner, range);

            Arrays.stream(splits).forEach(split -> assertThat(datastore.select(jobId, split)).isEmpty());
            Arrays.stream(splits).forEach(split -> datastore.insert(jobId, split, lower, upper, buf));
        }
        assertThat(datastore.store).hasSize(numNodes);

        for (int i = 0; i < tokens.size(); i++)
        {
            BigInteger lower = tokens.get(i);
            BigInteger upper = i == tokens.size() - 1 ? partitioner.maxToken() : tokens.get(i + 1);
            TokenRange range = TokenRange.openClosed(lower, upper);
            int[] splits = tokenSplitUtil.findOverlappingSplitIds(partitioner, range);
            ByteBuffer expected = buffers[i];
            Arrays.stream(splits).forEach(split -> assertByteBufferEquals(expected, datastore.selectBuffers(jobId, split).stream().findFirst().orElseThrow()));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 4, 8, 32, 128, 1024 })
    void testShrink(int numNodes)
    {
        Partitioner partitioner = Partitioner.Murmur3Partitioner;
        MockCdcStateV2 datastore = new MockCdcStateV2();
        String jobId = UUID.randomUUID().toString();
        CdcStatesSchema mockCdcStatesSchema = mock(CdcStatesSchema.class, RETURNS_DEEP_STUBS);
        TableHistorySchema mockTableHistorySchema = mock(TableHistorySchema.class, RETURNS_DEEP_STUBS);
        List<BigInteger> tokensBeforeShrink = TokenSplitUtil.splitTokens(numNodes, partitioner);
        List<BigInteger> tokensAfterShrink = TokenSplitUtil.splitTokens(numNodes / 2, partitioner);
        TokenSplitUtil tokenSplitUtil = new TokenSplitUtil(numNodes);

        Provider<TokenSplitUtil> tokenSplitUtilProvider = () -> tokenSplitUtil;

        CdcDatabaseAccessor db = new CdcDatabaseAccessor(mockCdcStatesSchema,
                                                         mockTableHistorySchema,
                                                         getMockCQLSessionProvider(datastore, mockCdcStatesSchema),
                                                         tokenSplitUtilProvider);

        ByteBuffer[] buffers = new ByteBuffer[tokensBeforeShrink.size()];
        for (int i = 0; i < tokensBeforeShrink.size(); i++)
        {
            BigInteger lower = tokensBeforeShrink.get(i);
            BigInteger upper = i == tokensBeforeShrink.size() - 1 ? partitioner.maxToken() : tokensBeforeShrink.get(i + 1);
            TokenRange range = TokenRange.openClosed(lower, upper);
            buffers[i] = randomBytes(i);
            int[] splits = tokenSplitUtil.findOverlappingSplitIds(partitioner, range);

            Arrays.stream(splits).forEach(split -> assertThat(datastore.select(jobId, split)).isEmpty());
            await(db.storeStateAsync(jobId, range, buffers[i], System.currentTimeMillis()).stream());
            List<byte[]> arrays = db.loadStateForRange(jobId, range).collect(Collectors.toList());
            assertThat(arrays).hasSize(1);
            assertByteBufferEquals(buffers[i], arrays.get(0));
        }
        assertThat(datastore.store).hasSize(numNodes);

        for (int i = 0; i < tokensAfterShrink.size(); i++)
        {
            BigInteger lower = tokensAfterShrink.get(i);
            BigInteger upper = i == tokensAfterShrink.size() - 1 ? partitioner.maxToken() : tokensAfterShrink.get(i + 1);
            TokenRange range = TokenRange.openClosed(lower, upper);
            List<byte[]> arrays = db.loadStateForRange(jobId, range).collect(Collectors.toList());
            assertThat(arrays).hasSize(2);
            assertByteBufferEquals(buffers[i * 2], arrays.get(0));
            assertByteBufferEquals(buffers[(i * 2) + 1], arrays.get(1));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 4, 8, 32, 128, 1024 })
    void testExpand(int numNodes)
    {
        Partitioner partitioner = Partitioner.Murmur3Partitioner;
        MockCdcStateV2 datastore = new MockCdcStateV2();
        String jobId = UUID.randomUUID().toString();
        CdcStatesSchema mockCdcStatesSchema = mock(CdcStatesSchema.class, RETURNS_DEEP_STUBS);
        TableHistorySchema mockTableHistorySchema = mock(TableHistorySchema.class, RETURNS_DEEP_STUBS);
        List<BigInteger> tokensBeforeExpansion = TokenSplitUtil.splitTokens(numNodes, partitioner);
        List<BigInteger> tokensAfterExpansion = TokenSplitUtil.splitTokens(numNodes * 2, partitioner);
        TokenSplitUtil tokenSplitUtil = new TokenSplitUtil(numNodes);

        Provider<TokenSplitUtil> tokenSplitUtilProvider = () -> tokenSplitUtil;

        CdcDatabaseAccessor db = new CdcDatabaseAccessor(mockCdcStatesSchema,
                                                         mockTableHistorySchema,
                                                         getMockCQLSessionProvider(datastore, mockCdcStatesSchema),
                                                         tokenSplitUtilProvider);

        ByteBuffer[] buffers = new ByteBuffer[tokensBeforeExpansion.size()];
        for (int i = 0; i < tokensBeforeExpansion.size(); i++)
        {
            BigInteger lower = tokensBeforeExpansion.get(i);
            BigInteger upper = i == tokensBeforeExpansion.size() - 1 ? partitioner.maxToken() : tokensBeforeExpansion.get(i + 1);
            TokenRange range = TokenRange.openClosed(lower, upper);
            buffers[i] = randomBytes(i);
            int[] splits = tokenSplitUtil.findOverlappingSplitIds(partitioner, range);

            Arrays.stream(splits).forEach(split -> assertThat(datastore.select(jobId, split)).isEmpty());
            await(db.storeStateAsync(jobId, range, buffers[i], System.currentTimeMillis()).stream());
            List<byte[]> arrays = db.loadStateForRange(jobId, range).collect(Collectors.toList());
            assertThat(arrays).hasSize(1);
            assertByteBufferEquals(buffers[i], arrays.get(0));
        }
        assertThat(datastore.store).hasSize(numNodes);

        for (int i = 0; i < tokensAfterExpansion.size(); i++)
        {
            BigInteger lower = tokensAfterExpansion.get(i);
            BigInteger upper = i == tokensAfterExpansion.size() - 1 ? partitioner.maxToken() : tokensAfterExpansion.get(i + 1);
            TokenRange range = TokenRange.openClosed(lower, upper);
            List<byte[]> arrays = db.loadStateForRange(jobId, range).collect(Collectors.toList());
            assertThat(arrays).hasSize(1);
            assertByteBufferEquals(buffers[i / 2], arrays.get(0));
        }
    }

    @Test
    void testOverlaps()
    {
        assertThat(overlaps(TokenRange.openClosed(BigInteger.ZERO, BigInteger.TEN), TokenRange.openClosed(BigInteger.ZERO, BigInteger.TEN))).isTrue();
        assertThat(overlaps(TokenRange.openClosed(BigInteger.ZERO, BigInteger.TEN), TokenRange.openClosed(BigInteger.ZERO, BigInteger.ZERO))).isFalse();
        assertThat(overlaps(TokenRange.openClosed(BigInteger.ZERO, BigInteger.TEN), TokenRange.openClosed(BigInteger.TEN, BigInteger.TEN))).isFalse();
        assertThat(overlaps(TokenRange.openClosed(BigInteger.ZERO, BigInteger.TEN), TokenRange.openClosed(BigInteger.ONE, BigInteger.TWO))).isTrue();
        assertThat(overlaps(TokenRange.openClosed(BigInteger.ZERO, BigInteger.TEN), TokenRange.openClosed(BigInteger.valueOf(3), BigInteger.valueOf(8)))).isTrue();
        assertThat(overlaps(TokenRange.openClosed(BigInteger.ZERO, BigInteger.TEN), TokenRange.openClosed(BigInteger.valueOf(-5), BigInteger.valueOf(5)))).isTrue();
        assertThat(overlaps(TokenRange.openClosed(BigInteger.ZERO, BigInteger.TEN), TokenRange.openClosed(BigInteger.valueOf(-5), BigInteger.TEN))).isTrue();
        assertThat(overlaps(TokenRange.openClosed(BigInteger.ZERO, BigInteger.TEN), TokenRange.openClosed(BigInteger.valueOf(-5), BigInteger.valueOf(15)))).isTrue();
        assertThat(overlaps(TokenRange.openClosed(BigInteger.ZERO, BigInteger.TEN), TokenRange.openClosed(BigInteger.valueOf(5), BigInteger.valueOf(15)))).isTrue();
        assertThat(overlaps(TokenRange.openClosed(BigInteger.ZERO, BigInteger.TEN), TokenRange.openClosed(BigInteger.valueOf(5), BigInteger.TEN))).isTrue();

        assertThat(overlaps(TokenRange.openClosed(BigInteger.ZERO, BigInteger.TEN), TokenRange.openClosed(BigInteger.valueOf(-5), BigInteger.valueOf(-1)))).isFalse();
        assertThat(overlaps(TokenRange.openClosed(BigInteger.ZERO, BigInteger.TEN), TokenRange.openClosed(BigInteger.valueOf(11), BigInteger.valueOf(15)))).isFalse();
    }

    // test utils

    private static class MockCdcStateV2
    {
        Map<Key, TreeMap<BigInteger, Map<BigInteger, Value>>> store = new HashMap<>();

        public void insert(String jobId, int split, BigInteger lower, BigInteger upper, ByteBuffer buf)
        {
            Key key = new Key(jobId, split);
            TreeMap<BigInteger, Map<BigInteger, Value>> partition = store.computeIfAbsent(key, (a) -> new TreeMap<>());
            partition.computeIfAbsent(lower, (a) -> new HashMap<>()).put(upper, new Value(lower, upper, buf));
        }

        public List<ByteBuffer> selectBuffers(String jobId, int split)
        {
            return select(jobId, split).stream().map(Value::buf).collect(Collectors.toList());
        }

        public List<Value> select(String jobId, int split)
        {
            Key key = new Key(jobId, split);
            TreeMap<BigInteger, Map<BigInteger, Value>> partition = store.get(key);
            if (partition == null || partition.isEmpty())
            {
                return List.of();
            }

            return partition.values().stream()
                            .flatMap(e -> e.entrySet().stream())
                            .map(Map.Entry::getValue)
                            .collect(Collectors.toList());
        }

        public boolean isEmpty()
        {
            return store.isEmpty();
        }
    }

    public static byte[] toByteArray(ByteBuffer buf)
    {
        buf.mark();
        byte[] ar = new byte[buf.remaining()];
        buf.get(ar);
        buf.reset();
        return ar;
    }

    /**
     * Key class for CDC state storage
     */
    public static class Key
    {
        final String jobId;
        final int split;

        public Key(String jobId, int split)
        {
            this.jobId = jobId;
            this.split = split;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;

            return split == key.split && Objects.equals(jobId, key.jobId);
        }

        public int hashCode()
        {
            return Objects.hash(jobId, split);
        }
    }

    /**
     * Value class for CDC state storage
     */
    public static class Value
    {
        final byte[] ar;
        final TokenRange range;

        public Value(TokenRange range, ByteBuffer buf)
        {
            this(range.lowerEndpoint(), range.upperEndpoint(), buf);
        }

        public Value(BigInteger lower, BigInteger upper, ByteBuffer buf)
        {
            this.range = TokenRange.openClosed(lower, upper);
            this.ar = toByteArray(buf);
        }

        public boolean overlaps(TokenRange range)
        {
            return TokenSplitUtil.overlaps(range, this.range);
        }

        public ByteBuffer buf()
        {
            return ByteBuffer.wrap(ar);
        }

        public byte[] ar()
        {
            return ar;
        }
    }

    private static ByteBuffer randomBytes(int prefix)
    {
        byte[] ar = new byte[64];
        ByteBuffer buf = ByteBuffer.wrap(ar);
        buf.putInt(prefix);
        byte[] random = new byte[60];
        ThreadLocalRandom.current().nextBytes(random);
        buf.put(random);
        return ByteBuffer.wrap(ar);
    }

    private static void assertByteBufferEquals(ByteBuffer buf1, byte[] ar)
    {
        assertByteBufferEquals(toByteArray(buf1), ar);
    }

    private static void assertByteBufferEquals(ByteBuffer buf1, ByteBuffer buf2)
    {
        assertByteBufferEquals(toByteArray(buf1), toByteArray(buf2));
    }

    private static void assertByteBufferEquals(byte[] ar1, byte[] ar2)
    {
        org.junit.jupiter.api.Assertions.assertArrayEquals(ar1, ar2);
    }

    /**
     * Argument provider for mock invocations
     */
    public static class ArgProvider
    {
        public Object[] args;

        @SuppressWarnings("unchecked")
        <T> T getArgument(int i)
        {
            return (T) args[i];
        }
    }

    private InstanceMetadataFetcher getMockInstanceMetaDataFetcher()
    {
        InstanceMetadata instanceMeta = mock(InstanceMetadata.class);
        InstanceMetadataFetcher instanceMetadataFetcher = mock(InstanceMetadataFetcher.class);
        when(instanceMetadataFetcher.instance(anyString())).thenReturn(instanceMeta);
        return instanceMetadataFetcher;
    }

    CQLSessionProvider getMockCQLSessionProvider(MockCdcStateV2 datastore, CdcStatesSchema mockCdcStatesSchema)
    {
        PreparedStatement insertStmt = mock(PreparedStatement.class);
        BoundStatement insertBound = mock(BoundStatement.class);
        ArgProvider insertArgs = new ArgProvider();
        when(insertStmt.bind(any())).thenAnswer(invocationOnMock -> {
            insertArgs.args = invocationOnMock.getArguments();
            return insertBound;
        });
        when(mockCdcStatesSchema.insertState()).thenReturn(insertStmt);

        PreparedStatement selectStmt = mock(PreparedStatement.class);
        BoundStatement selectBound = mock(BoundStatement.class);
        ArgProvider selectArgs = new ArgProvider();
        when(selectStmt.bind(any())).thenAnswer(invocationOnMock -> {
            selectArgs.args = invocationOnMock.getArguments();
            return selectBound;
        });
        when(mockCdcStatesSchema.select()).thenReturn(selectStmt);

        Session session = mock(Session.class, RETURNS_DEEP_STUBS);

        when(session.getCluster().getMetadata().getPartitioner()).thenReturn("org.apache.cassandra.dht.Murmur3Partitioner");

        // store inserts in mocked Datastore
        when(session.executeAsync(insertBound)).then(invocation -> {
            String jobId = insertArgs.getArgument(0);
            short split = insertArgs.getArgument(1);
            BigInteger lower = insertArgs.getArgument(2);
            BigInteger upper = insertArgs.getArgument(3);
            ByteBuffer buf = insertArgs.getArgument(4);
//            long timestamp = invocation.getArgument(5);
            datastore.insert(jobId, split, lower, upper, buf);
            return new TestResultSetFuture(mock(ResultSet.class));
        });

        when(session.executeAsync(selectBound)).then(invocation -> {
            String jobId = selectArgs.getArgument(0);
            short split = selectArgs.getArgument(1);
            List<Value> values = datastore.select(jobId, split);
            ResultSet resultSet = mock(ResultSet.class);
            List<Row> rows = values.stream().map(value -> {
                Row row = mock(Row.class);
                when(row.isNull(eq(0))).thenReturn(false);
                when(row.isNull(eq(1))).thenReturn(false);
                when(row.isNull(eq(2))).thenReturn(false);

                when(row.getVarint(eq(0))).thenReturn(value.range.lowerEndpoint());
                when(row.getVarint(eq(1))).thenReturn(value.range.upperEndpoint());
                when(row.getBytes(eq(2))).thenReturn(value.buf());
                return row;
            }).collect(Collectors.toList());
            when(resultSet.all()).thenReturn(rows);
            return new TestResultSetFuture(resultSet);
        });

        CQLSessionProvider cqlSession = mock(CQLSessionProvider.class);
        when(cqlSession.get()).thenReturn(session);
        when(cqlSession.getIfConnected()).thenReturn(session);
        return cqlSession;
    }

    private static class TestResultSetFuture implements ResultSetFuture
    {
        final ResultSet resultSet;

        public TestResultSetFuture(ResultSet resultSet)
        {
            this.resultSet = resultSet;
        }

        public ResultSet getUninterruptibly()
        {
            return resultSet;
        }

        public ResultSet getUninterruptibly(long timeout, TimeUnit unit)
        {
            return resultSet;
        }

        public boolean cancel(boolean mayInterruptIfRunning)
        {
            return false;
        }

        public boolean isCancelled()
        {
            return false;
        }

        public boolean isDone()
        {
            return true;
        }

        public ResultSet get()
        {
            return resultSet;
        }

        public ResultSet get(long timeout, @NotNull TimeUnit unit)
        {
            return resultSet;
        }

        public void addListener(Runnable listener, Executor executor)
        {

        }
    }
}

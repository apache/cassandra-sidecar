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

package org.apache.cassandra.sidecar.codecs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

import org.apache.commons.lang3.mutable.MutableInt;

import io.netty.util.CharsetUtil;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.tasks.ClusterTopologyMonitor;

/**
 * Message codec for encoding and decoding datacenter-local topology change events over the Vert.x event bus.
 */
public class DcLocalTopologyChangeEventCodec
        implements MessageCodec<ClusterTopologyMonitor.DcLocalTopologyChangeEvent,
                                ClusterTopologyMonitor.DcLocalTopologyChangeEvent>
{
    public static final DcLocalTopologyChangeEventCodec INSTANCE = new DcLocalTopologyChangeEventCodec();

    /**
     * Encodes a topology change event to the wire buffer.
     */
    public void encodeToWire(Buffer buf, ClusterTopologyMonitor.DcLocalTopologyChangeEvent event)
    {
        CommonCodecs.STRING.encodeToWire(buf, event.dc);
        encodeMap(buf, event.prev);
        encodeMap(buf, event.curr);
    }

    protected String decodeString(MutableInt pos, Buffer buf)
    {
        int len = buf.getInt(pos.intValue());
        pos.add(4);
        byte[] bytes = buf.getBytes(pos.intValue(), pos.intValue() + len);
        pos.add(len);
        return new String(bytes, CharsetUtil.UTF_8);
    }

    /**
     * Decodes a topology change event from the wire buffer.
     */
    public ClusterTopologyMonitor.DcLocalTopologyChangeEvent decodeFromWire(int pos, Buffer buf)
    {
        MutableInt mutablePos = new MutableInt(pos);
        String dc = decodeString(mutablePos, buf);
        Map<String, List<TokenRange>> prev = decodeMap(mutablePos, buf);
        Map<String, List<TokenRange>> curr = decodeMap(mutablePos, buf);

        return new ClusterTopologyMonitor.DcLocalTopologyChangeEvent(dc, prev, Objects.requireNonNull(curr, "Current topology is null"));
    }

    /**
     * Encodes a map of instance IP to token ranges.
     */
    protected static void encodeMap(Buffer buf, @Nullable Map<String, List<TokenRange>> map)
    {
        CommonCodecs.SHORT.encodeToWire(buf, (short) (map == null ? -1 : map.size())); // map size
        if (map != null)
        {
            for (Map.Entry<String, List<TokenRange>> entry : map.entrySet())
            {
                CommonCodecs.STRING.encodeToWire(buf, entry.getKey()); // instanceIP
                final List<TokenRange> ranges = entry.getValue();
                CommonCodecs.SHORT.encodeToWire(buf, (short) ranges.size()); // range size
                for (TokenRange range : ranges)
                {
                    RangeCodec.INSTANCE.encodeToWire(buf, range);
                }
            }
        }
    }

    /**
     * Decodes a map of instance IP to token ranges.
     */
    @Nullable
    protected static Map<String, List<TokenRange>> decodeMap(MutableInt pos, Buffer buf)
    {
        int mapSize = buf.getShort(pos.intValue());
        pos.add(2);
        if (mapSize < 0)
        {
            return null;
        }

        Map<String, List<TokenRange>> map = new HashMap<>(mapSize);
        for (int i = 0; i < mapSize; i++)
        {
            String instanceIp = INSTANCE.decodeString(pos, buf);

            int rangeSize = buf.getShort(pos.intValue());
            pos.add(2);

            List<TokenRange> ranges = new ArrayList<>(rangeSize);
            for (int j = 0; j < rangeSize; j++)
            {
                ranges.add(RangeCodec.INSTANCE.decodeFromWire(pos, buf));
            }
            map.put(instanceIp, ranges);
        }

        return map;
    }

    /**
     * Returns the event unchanged for local delivery.
     */
    public ClusterTopologyMonitor.DcLocalTopologyChangeEvent transform(ClusterTopologyMonitor.DcLocalTopologyChangeEvent
                                                                       event)
    {
        return event; // TopologyChangeEvent is immutable
    }

    /**
     * Returns the codec name.
     */
    public String name()
    {
        return "topology-change-event";
    }

    /**
     * Returns the system codec ID.
     */
    public byte systemCodecID()
    {
        return -1;
    }
}

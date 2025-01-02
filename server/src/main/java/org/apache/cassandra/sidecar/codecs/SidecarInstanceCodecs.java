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

import com.google.inject.Singleton;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;

/**
 * Codecs for Sidecar instances
 */
@Singleton
public class SidecarInstanceCodecs implements MessageCodec<InstanceMetadataImpl, InstanceMetadataImpl>
{
    @Override
    public void encodeToWire(Buffer buf, InstanceMetadataImpl instance)
    {
        buf.appendInt(instance.port());
        CommonCodecs.STRING.encodeToWire(buf, instance.host());
    }

    @Override
    public InstanceMetadataImpl decodeFromWire(int pos, Buffer buf)
    {
        final int port = buf.getInt(pos);
        pos += 4; // advance 4 bytes after reading int
        return InstanceMetadataImpl.builder()
                .host(CommonCodecs.STRING.decodeFromWire(pos, buf))
                .port(port)
                .build();
    }

    @Override
    public InstanceMetadataImpl transform(InstanceMetadataImpl instance)
    {
        return InstanceMetadataImpl.builder()
                .host(instance.host())
                .port(instance.port())
                .build();
    }

    @Override
    public String name()
    {
        return "SidecarInstance";
    }

    @Override
    public byte systemCodecID()
    {
        return -1;
    }
}

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

package org.apache.cassandra.sidecar.common.data;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.sidecar.common.data.GossipInfoResponse.GossipField.DC;
import static org.apache.cassandra.sidecar.common.data.GossipInfoResponse.GossipField.DISK_USAGE;
import static org.apache.cassandra.sidecar.common.data.GossipInfoResponse.GossipField.GENERATION;
import static org.apache.cassandra.sidecar.common.data.GossipInfoResponse.GossipField.HEARTBEAT;
import static org.apache.cassandra.sidecar.common.data.GossipInfoResponse.GossipField.HOST_ID;
import static org.apache.cassandra.sidecar.common.data.GossipInfoResponse.GossipField.INTERNAL_ADDRESS_AND_PORT;
import static org.apache.cassandra.sidecar.common.data.GossipInfoResponse.GossipField.INTERNAL_IP;
import static org.apache.cassandra.sidecar.common.data.GossipInfoResponse.GossipField.LOAD;
import static org.apache.cassandra.sidecar.common.data.GossipInfoResponse.GossipField.NATIVE_ADDRESS_AND_PORT;
import static org.apache.cassandra.sidecar.common.data.GossipInfoResponse.GossipField.NET_VERSION;
import static org.apache.cassandra.sidecar.common.data.GossipInfoResponse.GossipField.RACK;
import static org.apache.cassandra.sidecar.common.data.GossipInfoResponse.GossipField.RELEASE_VERSION;
import static org.apache.cassandra.sidecar.common.data.GossipInfoResponse.GossipField.REMOVAL_COORDINATOR;
import static org.apache.cassandra.sidecar.common.data.GossipInfoResponse.GossipField.RPC_ADDRESS;
import static org.apache.cassandra.sidecar.common.data.GossipInfoResponse.GossipField.RPC_READY;
import static org.apache.cassandra.sidecar.common.data.GossipInfoResponse.GossipField.SCHEMA;
import static org.apache.cassandra.sidecar.common.data.GossipInfoResponse.GossipField.SEVERITY;
import static org.apache.cassandra.sidecar.common.data.GossipInfoResponse.GossipField.SSTABLE_VERSIONS;
import static org.apache.cassandra.sidecar.common.data.GossipInfoResponse.GossipField.STATUS;
import static org.apache.cassandra.sidecar.common.data.GossipInfoResponse.GossipField.STATUS_WITH_PORT;
import static org.apache.cassandra.sidecar.common.data.GossipInfoResponse.GossipField.TOKENS;
import static org.apache.cassandra.sidecar.common.data.GossipInfoResponse.GossipField.read;

/**
 * A class representing the response of the cassandra gossip endpoint
 */
public class GossipInfoResponse extends HashMap<String, GossipInfoResponse.GossipInfo>
{
    /**
     * Overrides the {@link #get(Object)} method. The gossip info keys usually start with the format
     * {@code /ip:port}. Some clients may be unaware of the preceding {@code slash}, and lookups can
     * fail. This method attempts to lookup the value by prepending the {@code slash} at the beginning.
     * If the lookup fails, it defaults to the original behavior.
     *
     * @param key the key whose associated value is to be returned
     * @return {@link GossipInfo}
     */
    @Override
    public GossipInfo get(Object key)
    {
        if (key instanceof String)
        {
            String keyAsString = (String) key;
            if (keyAsString.length() > 0 && keyAsString.charAt(0) != '/')
            {
                GossipInfo value = super.get("/" + key);
                if (value != null)
                {
                    return value;
                }
            }
        }
        return super.get(key);
    }

    /**
     * Data accessor for reading Gossip states
     */
    public static class GossipInfo extends HashMap<String, String>
    {
        /**
         * Converts the key, if it is using the UPPER UNDERSCORE format, into camel case.
         * Then, put the new key and value.
         *
         * @param key   the key to convert
         * @param value the value for the gossip info entry
         */
        public void camelizeKeyAndPut(String key, String value)
        {
            String lowerCamelCasedKey = GossipField.valueOf(key.toUpperCase()).toLowerCamelCase();
            super.put(lowerCamelCasedKey, value);
        }

        @NotNull
        public String generation()
        {
            return read(this, GENERATION);
        }

        @NotNull
        public String heartbeat()
        {
            return read(this, HEARTBEAT);
        }

        @Nullable
        public String status()
        {
            return read(this, STATUS);
        }

        @Nullable
        public String load()
        {
            return read(this, LOAD);
        }

        /**
         * @return schema version, uuid string
         */
        @Nullable
        public String schema()
        {
            return read(this, SCHEMA);
        }

        /**
         * @return datacenter name
         */
        @Nullable
        public String datacenter()
        {
            return read(this, DC);
        }

        /**
         * @return rack name
         */
        @Nullable
        public String rack()
        {
            return read(this, RACK);
        }

        @Nullable
        public String releaseVersion()
        {
            return read(this, RELEASE_VERSION);
        }

        @Nullable
        public String removalCoordinator()
        {
            return read(this, REMOVAL_COORDINATOR);
        }

        @Nullable
        public String internalIp()
        {
            return read(this, INTERNAL_IP);
        }

        @Nullable
        public String rpcAddress()
        {
            return read(this, RPC_ADDRESS);
        }

        @Nullable
        public String severity()
        {
            return read(this, SEVERITY);
        }

        @Nullable
        public String netVersion()
        {
            return read(this, NET_VERSION);
        }

        @Nullable
        public String hostId()
        {
            return read(this, HOST_ID);
        }

        @Nullable
        public String tokens()
        {
            return read(this, TOKENS);
        }

        @Nullable
        public Boolean rpcReady()
        {
            String value = read(this, RPC_READY);
            return value == null ? null : Boolean.valueOf(value);
        }

        @Nullable
        public String internalAddressAndPort()
        {
            return read(this, INTERNAL_ADDRESS_AND_PORT);
        }

        @Nullable
        public String nativeAddressAndPort()
        {
            return read(this, NATIVE_ADDRESS_AND_PORT);
        }

        @Nullable
        public String statusWithPort()
        {
            return read(this, STATUS_WITH_PORT);
        }

        @Nullable
        public List<String> sstableVersions()
        {
            String value = read(this, SSTABLE_VERSIONS);
            return value == null ? null : Arrays.asList(value.split(","));
        }

        @Nullable
        public String diskUsage()
        {
            return read(this, DISK_USAGE);
        }
    }

    /**
     * Declares all fields that gossip info can possibly contain.
     *
     * <p>Note: When adding a new field, make sure there is a pairing access method defined in {@link GossipInfo}.
     */
    protected enum GossipField
    {
        GENERATION,
        HEARTBEAT,

        // Below are copied from org.apache.cassandra.gms.ApplicationState
        // Note: that all padding fields are not included.
        @Deprecated STATUS, //Deprecated and unused in 4.0, stop publishing in 5.0, reclaim in 6.0
        LOAD,
        SCHEMA,
        DC,
        RACK,
        RELEASE_VERSION,
        REMOVAL_COORDINATOR,
        @Deprecated INTERNAL_IP, //Deprecated and unused in 4.0, stop publishing in 5.0, reclaim in 6.0
        @Deprecated RPC_ADDRESS, // ^ Same
        SEVERITY,
        NET_VERSION,
        HOST_ID,
        TOKENS,
        RPC_READY,
        // pad to allow adding new states to existing cluster
        INTERNAL_ADDRESS_AND_PORT, //Replacement for INTERNAL_IP with up to two ports
        NATIVE_ADDRESS_AND_PORT, //Replacement for RPC_ADDRESS
        STATUS_WITH_PORT, //Replacement for STATUS
        /**
         * The set of sstable versions on this node. This will usually be only the "current" sstable format (the one
         * with which new sstables are written), but may contain more on newly upgraded nodes before `upgradesstable`
         * has been run.
         *
         * <p>The value (a set of sstable {@code org.apache.cassandra.io.sstable.format.VersionAndType}) is serialized
         * as a comma-separated list.
         **/
        SSTABLE_VERSIONS,
        DISK_USAGE;

        static String read(GossipInfo gossipInfo, GossipField field)
        {
            return gossipInfo.get(field.toLowerCamelCase());
        }

        /**
         * Returns the lower case camel version for the given {@code upperUnderscoreCase}. This method allows us not to
         * add a dependency to guava for the client, because guava can cause a lot of conflicts when used in libraries.
         *
         * @return the lower case camel version for the given {@code upperUnderscoreCase}
         */
        String toLowerCamelCase()
        {
            String upperUnderscoreCase = name();

            StringBuilder sb = new StringBuilder(upperUnderscoreCase.length());
            sb.append(Character.toLowerCase(upperUnderscoreCase.charAt(0)));

            int length = upperUnderscoreCase.length();
            for (int i = 1; i < length; i++)
            {
                if (upperUnderscoreCase.charAt(i) == '_' && i + 1 < length)
                {
                    i++;
                    sb.append(upperUnderscoreCase.charAt(i));
                }
                else
                {
                    sb.append(Character.toLowerCase(upperUnderscoreCase.charAt(i)));
                }
            }

            return sb.toString();
        }
    }
}

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

package org.apache.cassandra.sidecar.common.server.utils;

import java.util.List;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import org.apache.cassandra.sidecar.common.response.GossipInfoResponse;
import org.jetbrains.annotations.NotNull;

/**
 * Parses raw gossip info text into structured object, i.e. a map of host name to map of node details
 */
public class GossipInfoParser
{
    private GossipInfoParser()
    {
    } // util, do not instantiate

    private static final String IPV4_PATTERN = "(?:[0-9]{1,3}\\.){3}[0-9]{1,3}";
    private static final String IPV6_PATTERN = "(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}";
    // Cassandra wraps an IPv6 literal in square brackets before appending the port, so that the address and
    // the port can be told apart, e.g. '/[2001:db8:0:0:0:0:0:1]:7000'. The brackets are absent when the
    // gossip info is retrieved without ports. See org.apache.cassandra.locator.InetAddressAndPort#toString(InetAddress, int)
    private static final String BRACKETED_IPV6_PATTERN = String.format("\\[%s\\]", IPV6_PATTERN);
    // Pattern text matching both IPv4 and IPv6
    private static final String IP_PATTERN = String.format("((%s)|(%s)|(%s))",
                                                           IPV4_PATTERN, BRACKETED_IPV6_PATTERN, IPV6_PATTERN);
    // Pattern text matching IP with port optionally present.
    // Technically, [0-9]{1,5} represents a larger range for valid ports. It is fine for parsing the gossip info output.
    private static final String IP_WITH_PORT_PATTERN = String.format("%s(:[0-9]{1,5})?", IP_PATTERN);
    private static final String GOSSIP_INFO_FIELD_SEPARATOR = ":";
    // Matches 'HOSTNAME/IP:PORT', where the hostname and port are optional
    private static final String GOSSIP_INFO_HOST_HEADER_PATTERN = String.format("(.*)?/%s", IP_WITH_PORT_PATTERN);
    private static final Pattern GOSSIP_INFO_HOST_PATTERN = Pattern.compile(GOSSIP_INFO_HOST_HEADER_PATTERN);
    private static final Splitter GOSSIP_INFO_LINE_SPLITTER = Splitter.on(System.lineSeparator())
                                                                      .trimResults()
                                                                      .omitEmptyStrings();
    // Gossip info fields are of two formats
    // key:value
    // key:version:value
    // Add the limit(3) to make sure the value is not split if it contains any colons. Make sure
    // empty values are preserved.
    private static final Splitter GOSSIP_INFO_FIELD_SPLITTER = Splitter.on(GOSSIP_INFO_FIELD_SEPARATOR)
                                                                       .limit(3);

    /**
     * Parse raw gossip info into a map, where the key is the host name,
     * and the value is a map of all field keys associated with their values.
     *
     * @param rawGossipInfo the raw string retrieved from gossip
     * @return map parsed from raw gossip info.
     */
    public static GossipInfoResponse parse(@NotNull final String rawGossipInfo)
    {
        final GossipInfoResponse gossipInfoMap = new GossipInfoResponse();
        GossipInfoResponse.GossipInfo gossipInfo = null;

        for (String line : GOSSIP_INFO_LINE_SPLITTER.split(rawGossipInfo))
        {
            if (isGossipInfoHostHeader(line))
            {
                gossipInfo = gossipInfoMap.computeIfAbsent(line, s -> new GossipInfoResponse.GossipInfo());
            }
            else
            {
                Preconditions.checkState(gossipInfo != null,
                                         "Expected a gossip host header before the gossip fields. " +
                                         "Unrecognized line: %s", line);
                final List<String> splitLine = GOSSIP_INFO_FIELD_SPLITTER.splitToList(line);
                Preconditions.checkState(splitLine.size() == 2 || splitLine.size() == 3,
                                         "A gossip field should be split into two or three parts. %s",
                                         line);
                String key = splitLine.get(0);
                // ignore the version tag if present
                String value = splitLine.get(splitLine.size() - 1);
                gossipInfo.camelizeKeyAndPut(key, value);
            }
        }
        return gossipInfoMap;
    }

    /**
     * Matching '/IP:PORT' means it starts the info section for a new host.
     *
     * @param line the line from coming from gossip
     * @return true if matches; otherwise, false;
     */
    public static boolean isGossipInfoHostHeader(String line)
    {
        return GOSSIP_INFO_HOST_PATTERN.matcher(line).matches();
    }
}

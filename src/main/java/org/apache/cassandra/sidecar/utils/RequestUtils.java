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

package org.apache.cassandra.sidecar.utils;

/**
 * Utility class for Http request related operations.
 */
public class RequestUtils
{
    /**
     * Given a combined host address like 127.0.0.1:9042 or [2001:db8:0:0:0:ff00:42:8329]:9042, this method
     * removes port information and returns 127.0.0.1 or 2001:db8:0:0:0:ff00:42:8329.
     * @param address
     * @return host address without port information
     */
    public static String extractHostAddressWithoutPort(String address)
    {
        if (address.contains(":"))
        {
            // just ipv6 host name present without port information
            if (address.split(":").length > 2 && !address.startsWith("["))
            {
                return address;
            }
            String host = address.substring(0, address.lastIndexOf(':'));
            // remove brackets from ipv6 addresses
            return host.startsWith("[") ? host.substring(1, host.length() - 1) : host;
        }
        return address;
    }
}

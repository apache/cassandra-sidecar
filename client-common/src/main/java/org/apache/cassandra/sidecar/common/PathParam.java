/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.common;

/**
 * Simple data structure to hold both the simple and canonical names of a path parameter
 */
public class PathParam
{
    // The simple name does not start with ':'. Meanwhile, the canonical name does.
    // For example, the canonical name of a path param 'keyspace' is ':keyspace'.
    public final String simpleName;
    public final String canonicalName;

    public static PathParam of(String simpleName)
    {
        return new PathParam(simpleName);
    }

    private PathParam(String simpleName)
    {
        this.simpleName = simpleName;
        this.canonicalName = ':' + simpleName;
    }

    @Override
    public String toString()
    {
        return canonicalName;
    }
}

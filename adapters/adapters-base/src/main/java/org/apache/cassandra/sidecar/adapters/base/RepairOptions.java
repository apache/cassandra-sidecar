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

package org.apache.cassandra.sidecar.adapters.base;

/**
 * Enum representing the repair options supported.
 * This class mimics the options supported by the repair operation in Apache Cassandra
 * as defined in org.apache.cassandra.repair.messages.RepairOption
 * (<a href="https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/repair/messages/RepairOption.java">...</a>)
 */
public enum RepairOptions
{
    /**
     * Whether to repair only the primary range of the node (true/false)
     */
    PRIMARY_RANGE("primaryRange"),
    /**
     * Whether to perform an incremental repair (true/false)
     * If false, a full repair is performed
     */
    INCREMENTAL("incremental"),
    /**
     * Specific token ranges to repair
     */
    RANGES("ranges"),
    /**
     * List of column families (tables) to repair (comma-separated)
     */
    COLUMN_FAMILIES("columnFamilies"),
    /**
     * Restrict repair to specific data centers (comma-separated)
     */
    DATACENTERS("dataCenters"),
    /**
     * Restrict repair to specific hosts (comma-separated IPs or hostnames)
     */
    HOSTS("hosts"),
    /**
     * Force the repair operation
     */
    FORCE_REPAIR("forceRepair"),
    /**
     * Type of preview repair to run before actual repair
     * Options: "none", "auto", "running", "full"
     * Mainly used to assess data consistency without actually repairing
     */
    PREVIEW("previewKind");

    private final String value;

    RepairOptions(String value)
    {
        this.value = value;
    }

    /**
     * @return Name corresponding to the repair option
     */
    public String optionName()
    {
        return value;
    }
}

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

/**
 * Represents a Cassandra table
 */
public class Table extends Name
{
    /**
     * Constructs a {@link Table} object with the provided {@code name} for the table.
     *
     * @param name the name of the Cassandra table
     */
    public Table(String name)
    {
        super(name);
    }

    /**
     * Constructs a {@link Table} object with the provided {@code unquotedName} for the table, and
     * {@code isQuotedFromSource} parameters. For example, if the user-provided input was
     * {@code "\"QuotedTableName\""}, then the parameters for this constructor should be initialized with
     * {@code new Table("QuotedTableName", true)}.
     *
     * @param unquotedName       the unquoted name for the table
     * @param isQuotedFromSource whether the name was quoted from source
     */
    public Table(String unquotedName, boolean isQuotedFromSource)
    {
        super(unquotedName, isQuotedFromSource);
    }
}

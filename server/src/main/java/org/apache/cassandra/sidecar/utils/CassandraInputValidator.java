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

import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.exceptions.CassandraInputException;
import org.jetbrains.annotations.NotNull;

/**
 * Interface that defines different Cassandra input validations
 */
public interface CassandraInputValidator
{
    /**
     * Validates that the {@code keyspace} is not {@code null}, that it contains valid characters, and that it's
     * not a forbidden keyspace.
     *
     * @param keyspace the name of the Cassandra keyspace to validate
     * @return the validated {@code keyspace}
     * @throws NullPointerException when the {@code keyspace} is {@code null}
     */
    Name validateKeyspaceName(@NotNull String keyspace) throws NullPointerException;

    /**
     * Validates that the {@code tableName} is not {@code null}, and it contains allowed character for Cassandra
     * table names.
     *
     * @param tableName the name of the Cassandra table to validate
     * @return the validated {@code tableName}
     * @throws NullPointerException    when the {@code tableName} is {@code null}
     * @throws CassandraInputException when the {@code tableName} contains invalid characters in the name
     */
    Name validateTableName(@NotNull String tableName);

    /**
     * Validates that the {@code snapshotName} is not {@code null}, and it contains allowed character for the
     * Cassandra snapshot names.
     *
     * @param snapshotName the name of the Cassandra snapshot to validate
     * @return the validated {@code snapshotName}
     * @throws NullPointerException    when the {@code snapshotName} is {@code null}
     * @throws CassandraInputException when the {@code snapshotName} contains invalid characters in the name
     */
    String validateSnapshotName(@NotNull String snapshotName);

    /**
     * Validates that the {@code componentName} is not {@code null}, and it contains allowed names for the
     * Cassandra SSTable component.
     *
     * @param componentName the name of the SSTable component to validate
     * @return the validated {@code componentName}
     * @throws NullPointerException    when the {@code componentName} is null
     * @throws CassandraInputException when the {@code componentName} is not valid
     */
    String validateComponentName(@NotNull String componentName);

    /**
     * Validates that the {@code componentName} is not {@code null}, and it contains a subset of allowed names for the
     * Cassandra SSTable component.
     *
     * @param componentName the name of the SSTable component to validate
     * @return the validated {@code componentName}
     * @throws NullPointerException    when the {@code componentName} is null
     * @throws CassandraInputException when the {@code componentName} is not a valid name for the configured restricted
     *                                 component name
     */
    String validateRestrictedComponentName(@NotNull String componentName);

    /**
     * Validates that the index name has only valid characters
     *
     * @param secondaryIndexName the name of the secondary index
     */
    void validateIndexName(String secondaryIndexName);

    /**
     * Validates that the unique table identifier is a valid hexadecimal
     *
     * @param tableId the table identifier to validate
     */
    void validateTableId(String tableId);

    /**
     * Validates that the {@code name} matches the name pattern
     *
     * @param name          name
     * @param exceptionHint hint to add in the exception message
     * @throws CassandraInputException when the {@code unquotedInput} does not match the pattern
     */
    void validateNamePattern(Name name, String exceptionHint);
}

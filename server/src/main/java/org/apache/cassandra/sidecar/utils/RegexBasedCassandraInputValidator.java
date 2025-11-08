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

import java.io.File;
import java.util.Objects;

import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.config.CassandraInputValidationConfiguration;
import org.apache.cassandra.sidecar.config.yaml.CassandraInputValidationConfigurationImpl;
import org.apache.cassandra.sidecar.exceptions.CassandraInputException;
import org.apache.cassandra.sidecar.exceptions.ForbiddenCassandraInputException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Miscellaneous methods used for validation.
 */
public class RegexBasedCassandraInputValidator implements CassandraInputValidator
{
    protected final CassandraInputValidationConfiguration validationConfiguration;

    @VisibleForTesting
    RegexBasedCassandraInputValidator()
    {
        this(new CassandraInputValidationConfigurationImpl());
    }

    /**
     * Constructs a new object with the provided {@code validationConfiguration}
     *
     * @param validationConfiguration a validation configuration
     */
    public RegexBasedCassandraInputValidator(CassandraInputValidationConfiguration validationConfiguration)
    {
        this.validationConfiguration = validationConfiguration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Name validateKeyspaceName(@NotNull String keyspace)
    {
        Name name = new Name(keyspace);

        if (validationConfiguration.forbiddenKeyspaces().contains(name.name()))
            throw new ForbiddenCassandraInputException("Forbidden keyspace: " + keyspace);

        validateNamePattern(name, "keyspace");

        return name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Name validateTableName(@NotNull String tableName)
    {
        Name name = new Name(tableName);
        validateNamePattern(name, "table name");
        return name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String validateSnapshotName(@NotNull String snapshotName)
    {
        Objects.requireNonNull(snapshotName, "snapshotName must not be null");
        //  most UNIX systems only disallow file separator and null characters for directory names
        if (snapshotName.contains(File.separator) || snapshotName.contains("\0"))
            throw new CassandraInputException("Invalid characters in snapshot name: " + snapshotName);
        return snapshotName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String validateComponentName(@NotNull String componentName)
    {
        return validateComponentNameByRegex(componentName,
                                            validationConfiguration.allowedPatternForComponentName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String validateRestrictedComponentName(@NotNull String componentName)
    {
        return validateComponentNameByRegex(componentName,
                                            validationConfiguration.allowedPatternForRestrictedComponentName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void validateIndexName(String secondaryIndexName)
    {
        Preconditions.checkArgument(!secondaryIndexName.isEmpty(), "secondaryIndexName cannot be empty");
        Preconditions.checkArgument(secondaryIndexName.charAt(0) == '.', "Invalid secondary index name");
        String indexName = secondaryIndexName.substring(1);
        validatePattern(indexName, indexName, "secondary index", false);
    }

    /**
     * {@inheritDoc}
     */
    @NotNull
    private String validateComponentNameByRegex(String componentName, String regex)
    {
        Objects.requireNonNull(componentName, "componentName must not be null");
        if (!componentName.matches(regex))
            throw new CassandraInputException("Invalid component name: " + componentName);
        return componentName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void validateTableId(String tableId)
    {
        Objects.requireNonNull(tableId, "tableId must not be null");
        Preconditions.checkArgument(tableId.length() <= 32, "tableId cannot be longer than 32 characters");
        for (int i = 0; i < tableId.length(); i++)
        {
            char c = tableId.charAt(i);
            if (!isHex(c))
                throw new CassandraInputException("Invalid characters in table id: " + tableId);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void validateNamePattern(Name name, String exceptionHint)
    {
        validatePattern(name.name(), name.maybeQuotedName(), exceptionHint, name.isSourceQuoted());
    }

    /**
     * Validates that the {@code unquotedInput} matches the {@code patternWordChars}
     *
     * @param unquotedInput      the unquoted input
     * @param maybeQuoted        the original input used for the exception message
     * @param exceptionHint      hint to add in the exception message
     * @param isQuotedFromSource whether the name was quoted from source
     * @throws CassandraInputException when the {@code unquotedInput} does not match the pattern
     */
    protected void validatePattern(String unquotedInput, String maybeQuoted,
                                   String exceptionHint, boolean isQuotedFromSource)
    {
        String pattern = isQuotedFromSource
                         ? validationConfiguration.allowedPatternForQuotedName()
                         : validationConfiguration.allowedPatternForName();

        if (!unquotedInput.matches(pattern))
            throw new CassandraInputException("Invalid characters in " + exceptionHint + ": " + maybeQuoted);
    }

    /**
     * @param c the character to test
     * @return {@code true} if the input {@code c} is valid hexadecimal, {@code false} otherwise
     */
    protected boolean isHex(char c)
    {
        return (c >= 'a' && c <= 'f') || (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F');
    }
}

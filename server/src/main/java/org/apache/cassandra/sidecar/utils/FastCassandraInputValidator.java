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

import java.util.List;
import java.util.Map;
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
 * An implementation of the {@link CassandraInputValidator} that does not use regular expressions
 * for validations and uses optimized validations.
 */
public class FastCassandraInputValidator extends RegexBasedCassandraInputValidator
{
    /**
     * Longest permissible keyspace name
     */
    public static final int KEYSPACE_NAME_LENGTH = 48;
    /**
     * Longest permissible table name. See CASSANDRA-20389 (Create table fails on long table names) for
     * details about the maximum length for table names.
     */
    public static final int TABLE_NAME_LENGTH = 222;
    /**
     * Default valid component name terminations
     */
    public static final List<String> DEFAULT_VALID_TERMINATIONS = List.of(".db", ".cql", ".json", ".crc32", "TOC.txt");

    /**
     * Default valid component name terminations for restricted component names
     */
    public static final List<String> DEFAULT_VALID_RESTRICTED_TERMINATIONS = List.of(".db", "TOC.txt");

    @VisibleForTesting
    final List<String> validTerminations;
    @VisibleForTesting
    final List<String> validRestrictedTerminations;

    @VisibleForTesting
    public FastCassandraInputValidator()
    {
        this(new CassandraInputValidationConfigurationImpl());
    }

    /**
     * Constructs a new object with the provided {@code validationConfiguration}
     *
     * @param validationConfiguration a validation configuration
     */
    public FastCassandraInputValidator(CassandraInputValidationConfiguration validationConfiguration)
    {
        super(validationConfiguration);
        Map<String, String> configMap = validationConfiguration.validatorConfiguration().namedParameters();
        validTerminations = parseConfiguredOrDefault(configMap, "valid_terminations", DEFAULT_VALID_TERMINATIONS);
        validRestrictedTerminations = parseConfiguredOrDefault(configMap, "valid_restricted_terminations", DEFAULT_VALID_RESTRICTED_TERMINATIONS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Name validateKeyspaceName(@NotNull String keyspace) throws NullPointerException
    {
        Name name = new Name(keyspace);
        validateNameLength(name, "keyspace", KEYSPACE_NAME_LENGTH);
        validateNamePattern(name, "keyspace");

        if (validationConfiguration.forbiddenKeyspaces().contains(name.name()))
            throw new ForbiddenCassandraInputException("Forbidden keyspace: " + keyspace);

        return name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Name validateTableName(@NotNull String tableName)
    {
        Name name = new Name(tableName);
        validateNameLength(name, "table name", TABLE_NAME_LENGTH);
        validateNamePattern(name, "table name");
        return name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String validateComponentName(@NotNull String componentName)
    {
        Objects.requireNonNull(componentName, "componentName must not be null");
        Preconditions.checkArgument(!componentName.isEmpty(), () -> "componentName cannot be empty");
        validateComponentName(componentName, validTerminations);
        return componentName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String validateRestrictedComponentName(@NotNull String componentName)
    {
        Objects.requireNonNull(componentName, "componentName must not be null");
        Preconditions.checkArgument(!componentName.isEmpty(), () -> "componentName cannot be empty");
        validateComponentName(componentName, validRestrictedTerminations);
        return componentName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void validateIndexName(String secondaryIndexName)
    {
        Preconditions.checkArgument(!secondaryIndexName.isEmpty(), "secondaryIndexName cannot be empty");
        if (secondaryIndexName.charAt(0) != '.')
            throw new CassandraInputException("Invalid secondary index name: " + secondaryIndexName);
        validateNamePattern(secondaryIndexName, secondaryIndexName, false, "secondary index", 1 /* skip the first character */);
    }

    /**
     * Validates that the {@code name} is a valid name in Cassandra as defined by the grammar in
     * <a href="https://cassandra.apache.org/doc/4.1/cassandra/cql/ddl.html#common-definitions">Cassandra CQL common
     * definitions</a>
     *
     * @param name          name to validate
     * @param exceptionHint hint to add in the exception message
     * @throws CassandraInputException when the {@code unquotedInput} has invalid characters
     */
    @Override
    public void validateNamePattern(Name name, String exceptionHint)
    {
        validateNamePattern(name.name(), name.maybeQuotedName(), name.isSourceQuoted(), exceptionHint, 0);
    }

    /**
     * Validates that the {@code name} is a valid name in Cassandra as defined by the grammar in
     * <a href="https://cassandra.apache.org/doc/4.1/cassandra/cql/ddl.html#common-definitions">Cassandra CQL common
     * definitions</a>. The validation will only take into account the start index.
     *
     * @param unquotedName    the unquoted name to validate
     * @param maybeQuotedName the name that maybe quoted
     * @param isSourceQuoted  whether the source will be quoted
     * @param exceptionHint   hint to add in the exception message
     * @param startIndex      start index
     * @throws CassandraInputException when the {@code unquotedName} has invalid characters
     */
    protected void validateNamePattern(String unquotedName, String maybeQuotedName, boolean isSourceQuoted,
                                       String exceptionHint, int startIndex)
    {
        char c;
        if (!isSourceQuoted)
        {
            // Validate the first character. Unquoted names can only begin with a letter
            c = unquotedName.charAt(startIndex++);
            if (!isLetter(c))
                throw new CassandraInputException("Invalid characters in " + exceptionHint + ": " + maybeQuotedName);
        }

        while (startIndex < unquotedName.length())
        {
            c = unquotedName.charAt(startIndex++);
            if (!isAlphanumeric(c) && !isUnderscore(c))
                throw new CassandraInputException("Invalid characters in " + exceptionHint + ": " + maybeQuotedName);
        }
    }

    /**
     * Validates that the {@code name} has valid length
     *
     * @param name          name to validate
     * @param exceptionHint hint to add in the exception message
     * @param maxNameLength the maximum length for the name
     * @throws CassandraInputException when the length of the {@code unquotedInput} is empty or larger than
     *                                 {@code maxNameLength}
     */
    protected void validateNameLength(Name name, String exceptionHint, int maxNameLength)
    {
        String unquotedInput = name.name();
        if (unquotedInput.isEmpty() || unquotedInput.length() > maxNameLength)
            throw new CassandraInputException("Invalid length " + unquotedInput.length() +
                                              " for " + exceptionHint + ": " + name.maybeQuotedName());
    }

    /**
     * Validates that the {@code componentName} has valid characters, and it ends with one of the
     * {@code validTerminations}.
     *
     * @param componentName     the name of the component to validate
     * @param validTerminations a list of valid terminations for the component name
     */
    protected void validateComponentName(String componentName, List<String> validTerminations)
    {
        char c;
        int lastIndexOfAllowedTermination = lastIndexOfAllowedTermination(componentName, validTerminations);

        if (lastIndexOfAllowedTermination < 1)
            throw new CassandraInputException("Invalid component name: " + componentName);

        for (int i = lastIndexOfAllowedTermination - 1; i >= 0; i--)
        {
            c = componentName.charAt(i);
            if (!isValidComponentNameCharacter(c))
                throw new CassandraInputException("Invalid component name: " + componentName);
        }
    }

    /**
     * @param componentName     the name of the component to validate
     * @param validTerminations a list of valid terminations for the component name
     * @return the last index of the first found allowed termination from the provided list of
     * {@code validTerminations}, or {@code -1} if no index is found in any of the {@code validTerminations}
     */
    protected int lastIndexOfAllowedTermination(String componentName, List<String> validTerminations)
    {
        for (String allowedExtension : validTerminations)
        {
            if (componentName.endsWith(allowedExtension))
                return componentName.length() - allowedExtension.length();
        }
        return -1;
    }

    /**
     * @param c the character to test
     * @return {@code true} if the input {@code c} is a valid alphanumeric character, underscore, or a dash;
     * {@code false} otherwise
     */
    protected boolean isValidComponentNameCharacter(char c)
    {
        return isAlphanumeric(c) || isUnderscore(c) || isDash(c);
    }

    /**
     * @param c the character to test
     * @return {@code true} if the input {@code c} is a valid alphanumeric character, {@code false} otherwise
     */
    protected boolean isAlphanumeric(char c)
    {
        return (c >= '0' && c <= '9') || isLetter(c);
    }

    /**
     * @param c the character to test
     * @return {@code true} if the input {@code c} is a valid letter, {@code false} otherwise
     */
    protected boolean isLetter(char c)
    {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    }

    /**
     * @param c the character to test
     * @return {@code true} if the input {@code c} is an underscore, {@code false} otherwise
     */
    protected boolean isUnderscore(char c)
    {
        return c == '_';
    }

    /**
     * @param c the character to test
     * @return {@code true} if the input {@code c} is an underscore, {@code false} otherwise
     */
    protected boolean isDash(char c)
    {
        return c == '-';
    }

    /**
     * @param configMap    the configuration map
     * @param key          the key in the map
     * @param defaultValue the default values to provide when the value is not available for parsing
     * @return the parsed value, or the default value when unavailable
     */
    static List<String> parseConfiguredOrDefault(Map<String, String> configMap, String key, List<String> defaultValue)
    {
        if (configMap != null)
        {
            String value = configMap.get(key);
            if (value != null && !value.isEmpty())
            {
                return List.of(value.split(","));
            }
        }
        return defaultValue;
    }
}

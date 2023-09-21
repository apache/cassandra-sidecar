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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.config.CassandraInputValidationConfiguration;
import org.apache.cassandra.sidecar.config.yaml.CassandraInputValidationConfigurationImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Miscellaneous methods used for validation.
 */
@Singleton
public class CassandraInputValidator
{
    private final CassandraInputValidationConfiguration validationConfiguration;

    /**
     * Constructs a new object with the provided {@code validationConfiguration}
     *
     * @param validationConfiguration a validation configuration
     */
    @Inject
    public CassandraInputValidator(CassandraInputValidationConfiguration validationConfiguration)
    {
        this.validationConfiguration = validationConfiguration;
    }

    @VisibleForTesting
    public CassandraInputValidator()
    {
        this(new CassandraInputValidationConfigurationImpl());
    }

    /**
     * Validates that the {@code keyspace} is not {@code null}, that it contains valid characters, and that it's
     * not a forbidden keyspace.
     *
     * @param keyspace the name of the Cassandra keyspace to validate
     * @return the validated {@code keyspace}
     * @throws NullPointerException when the {@code keyspace} is {@code null}
     * @throws HttpException        when the {@code keyspace} contains invalid characters in the name or when the
     *                              keyspace is forbidden
     */
    public String validateKeyspaceName(@NotNull String keyspace)
    {
        Objects.requireNonNull(keyspace, "keyspace must not be null");
        validatePattern(keyspace, "keyspace");
        if (validationConfiguration.forbiddenKeyspaces().contains(keyspace))
            throw new HttpException(HttpResponseStatus.FORBIDDEN.code(), "Forbidden keyspace: " + keyspace);
        return keyspace;
    }

    /**
     * Validates that the {@code tableName} is not {@code null}, and it contains allowed character for Cassandra
     * table names.
     *
     * @param tableName the name of the Cassandra table to validate
     * @return the validated {@code tableName}
     * @throws NullPointerException when the {@code tableName} is {@code null}
     * @throws HttpException        when the {@code tableName} contains invalid characters in the name
     */
    public String validateTableName(@NotNull String tableName)
    {
        Objects.requireNonNull(tableName, "tableName must not be null");
        validatePattern(tableName, "table name");
        return tableName;
    }

    /**
     * Validates that the {@code snapshotName} is not {@code null}, and it contains allowed character for the
     * Cassandra snapshot names.
     *
     * @param snapshotName the name of the Cassandra snapshot to validate
     * @return the validated {@code snapshotName}
     * @throws NullPointerException when the {@code snapshotName} is {@code null}
     * @throws HttpException        when the {@code snapshotName} contains inalid characters in the name
     */
    public String validateSnapshotName(@NotNull String snapshotName)
    {
        Objects.requireNonNull(snapshotName, "snapshotName must not be null");
        //  most UNIX systems only disallow file separator and null characters for directory names
        if (snapshotName.contains(File.separator) || snapshotName.contains("\0"))
            throw new HttpException(HttpResponseStatus.BAD_REQUEST.code(),
                                    "Invalid characters in snapshot name: " + snapshotName);
        return snapshotName;
    }

    /**
     * Validates that the {@code componentName} is not {@code null}, and it contains allowed names for the
     * Cassandra SSTable component.
     *
     * @param componentName the name of the SSTable component to validate
     * @return the validated {@code componentName}
     * @throws NullPointerException when the {@code componentName} is null
     * @throws HttpException        when the {@code componentName} is not valid
     */
    public String validateComponentName(@NotNull String componentName)
    {
        return validateComponentNameByRegex(componentName,
                                            validationConfiguration.allowedPatternForComponentName());
    }

    /**
     * Validates that the {@code componentName} is not {@code null}, and it contains a subset of allowed names for the
     * Cassandra SSTable component.
     *
     * @param componentName the name of the SSTable component to validate
     * @return the validated {@code componentName}
     * @throws NullPointerException when the {@code componentName} is null
     * @throws HttpException        when the {@code componentName} is not a valid name for the configured restricted
     *                              component name
     */
    public String validateRestrictedComponentName(@NotNull String componentName)
    {
        return validateComponentNameByRegex(componentName,
                                            validationConfiguration.allowedPatternForRestrictedComponentName());
    }

    /**
     * Validates the {@code componentName} against the provided {@code regex}.
     *
     * @param componentName the name of the SSTable component
     * @param regex         the regex for validation
     * @return the validated {@code componentName}
     * @throws NullPointerException when the {@code componentName} is null
     * @throws HttpException        when the {@code componentName} does not match the provided regex
     */
    @NotNull
    private String validateComponentNameByRegex(String componentName, String regex)
    {
        Objects.requireNonNull(componentName, "componentName must not be null");

        if (isIndexComponent(componentName))
        {
            String[] split = componentName.split("/");

            if (split.length != 2)
                throw HttpExceptions.wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                                       "Invalid index component name: " + componentName);

            // validate the index directory
            validatePattern(split[0].substring(1), "index");
            if (!split[1].matches(regex))
                throw HttpExceptions.wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                                       "Invalid component name: " + componentName);
        }
        else
        {
            if (!componentName.matches(regex))
                throw new HttpException(HttpResponseStatus.BAD_REQUEST.code(),
                                        "Invalid component name: " + componentName);
        }
        return componentName;
    }

    /**
     * Returns {@code true} if the provided {@code componentName} is a secondary index SSTable component,
     * {@code false} otherwise.
     *
     * @param componentName the name of the SSTable component
     * @return {@code true} if the provided {@code componentName} is a secondary index SSTable component,
     * {@code false} otherwise.
     */
    private boolean isIndexComponent(@NotNull String componentName)
    {
        return componentName.startsWith(".") && componentName.contains("/");
    }

    /**
     * Validates that the {@code input} matches the {@code patternWordChars}
     *
     * @param input the input
     * @param name  a name for the exception
     * @throws HttpException when the {@code input} does not match the pattern
     */
    private void validatePattern(String input, String name)
    {
        if (!input.matches(validationConfiguration.allowedPatternForDirectory()))
            throw new HttpException(HttpResponseStatus.BAD_REQUEST.code(),
                                    "Invalid characters in " + name + ": " + input);
    }
}

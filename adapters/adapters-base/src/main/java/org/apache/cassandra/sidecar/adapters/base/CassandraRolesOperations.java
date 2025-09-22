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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.sidecar.common.request.data.GenerateRoleRequestPayload;
import org.apache.cassandra.sidecar.common.response.GenerateRoleResponse;
import org.apache.cassandra.sidecar.common.server.ICassandraAdapter;
import org.apache.cassandra.sidecar.common.server.RolesOperations;

public class CassandraRolesOperations implements RolesOperations
{
    private final UUIDGenerator generator;
    private final ICassandraAdapter cassandraAdapter;

    public CassandraRolesOperations(ICassandraAdapter cassandraAdapter)
    {
        this.cassandraAdapter = cassandraAdapter;
        this.generator = new UUIDGenerator();
    }

    public GenerateRoleResponse generateRole(GenerateRoleRequestPayload payload)
    {
        String role = null;
        String password = null;
        if (payload.sidecarGeneration)
        {
            role = generator.generate(payload.roleNameOptions);
            // we do not accept any parameters for password, it will be just uuid
            if (payload.passwordGeneration)
                password = generator.generate(Map.of());
        }

        try
        {
            String query;
            if (payload.sidecarGeneration)
            {
                query = CqlRoleBuilder.createRole(role)
                                      .password(password)
                                      .login(payload.login)
                                      .superuser(payload.superuser)
                                      .build();
            }
            else
            {
                query = CqlRoleBuilder.createGeneratedRole()
                                      .generatedPassword(payload.passwordGeneration)
                                      .login(payload.login)
                                      .superuser(payload.superuser)
                                      .option(payload.roleNameOptions)
                                      .build();
            }

            ResultSet resultSet = cassandraAdapter.executeLocal(new SimpleStatement(query));

            ExecutionInfo executionInfo = resultSet.getExecutionInfo();
            List<String> warnings = executionInfo.getWarnings();
            if (warnings != null && !warnings.isEmpty())
                throw new IllegalStateException(String.join(" ", warnings));

            if (payload.sidecarGeneration)
            {
                return new GenerateRoleResponse(role, password);
            }
            else
            {
                Row one = resultSet.one();
                return new GenerateRoleResponse(one.getString("generated_role"), one.getString("generated_password"));
            }
        }
        catch (Throwable t)
        {
            throw new IllegalStateException(t);
        }
    }

    /**
     * Generator of UUIDs where first character is always a character.
     */
    @VisibleForTesting
    public static class UUIDGenerator
    {
        public static final String NAME_PREFIX_KEY = "name_prefix";
        public static final String NAME_SUFFIX_KEY = "name_suffix";
        public static final String NAME_SIZE = "name_size";
        public static final int MINIMUM_NAME_SIZE = 10;

        private static final Pattern PATTERN = Pattern.compile("-");
        public static final char[] FIRST_CHARS = { 'a', 'b', 'c', 'd', 'e', 'f' };

        private static final Random random = new Random();
        // lenght of UUID without hyphens
        // generated name can be longer than this if it has prefix / suffix
        public static final int MAXIMUM_NAME_SIZE = 32;

        public String generate(Map<String, String> options)
        {
            int size = getSize(options);

            // to always start on a letter, so we do not need to wrap in ''
            char firstChar = FIRST_CHARS[random.nextInt(6)];
            String uuid = UUID.randomUUID().toString().toLowerCase(Locale.ROOT);
            String uuidWithoutHyphens = PATTERN.matcher(uuid).replaceAll("");
            String name = firstChar + uuidWithoutHyphens.substring(1);

            name = name.substring(0, size);
            name = enrich(NAME_PREFIX_KEY, name, options);
            name = enrich(NAME_SUFFIX_KEY, name, options);

            return name;
        }

        private String enrich(String key, String generatedValue, Map<String, String> options)
        {
            if (options == null || options.isEmpty())
                return generatedValue;

            if (options.containsKey(key))
            {
                Object value = options.get(key);

                if (value == null)
                    throw new IllegalArgumentException("Value of " + key + " cannot be null.");

                if (NAME_PREFIX_KEY.equals(key))
                    generatedValue = value + generatedValue;
                else if (NAME_SUFFIX_KEY.equals(key))
                    generatedValue = generatedValue + value;
            }

            return generatedValue;
        }

        private int getSize(Map<String, String> options)
        {
            Object sizeObject;
            if (options == null)
            {
                sizeObject = MAXIMUM_NAME_SIZE;
            }
            else if (options.containsKey(NAME_SIZE))
            {
                Object nameSizeValue = options.get(NAME_SIZE);
                if (nameSizeValue != null)
                    sizeObject = nameSizeValue;
                else
                    throw new IllegalArgumentException("Value of " + NAME_SIZE + " has to be strictly positive integer.");
            }
            else
            {
                sizeObject = MAXIMUM_NAME_SIZE;
            }

            int size;

            if (sizeObject instanceof String)
            {
                try
                {
                    size = Integer.parseInt((String) sizeObject);
                }
                catch (Throwable t)
                {
                    throw new IllegalArgumentException("Value '" + sizeObject + "' can't be converted to integer.");
                }
            }
            else size = ((Number) sizeObject).intValue();

            if (size < MINIMUM_NAME_SIZE)
                throw new IllegalArgumentException("Value of " + NAME_SIZE + " parameter has to be at least " + MINIMUM_NAME_SIZE + '.');

            if (size > MAXIMUM_NAME_SIZE)
                throw new IllegalArgumentException("Generator generates names of maximum length " + MAXIMUM_NAME_SIZE + ". " +
                                                   "You want to generate with length " + size + '.');

            return size;
        }
    }

    public static class CqlRoleBuilder
    {
        private final String roleName;
        private String password;
        private boolean login;
        private boolean superuser;
        private final Map<String, Object> customOptions = new LinkedHashMap<>();
        private boolean ifNotExists = false;
        private boolean generateRole = false;
        private boolean generatePassword = false;

        private CqlRoleBuilder(boolean generatedRole)
        {
            this.generateRole = generatedRole;
            this.roleName = null;
        }

        private CqlRoleBuilder(String roleName)
        {
            this.roleName = Objects.requireNonNull(roleName, "Role name cannot be null");
        }

        /**
         * Start building a CREATE ROLE statement
         *
         * @param roleName the name of the role to create
         * @return new builder instance
         */
        public static CqlRoleBuilder createRole(String roleName)
        {
            return new CqlRoleBuilder(roleName);
        }

        /**
         * Start building a CREATE GENERATED ROLE statement.
         *
         * @return new builder instance
         */
        public static CqlRoleBuilder createGeneratedRole()
        {
            return new CqlRoleBuilder(true);
        }

        /**
         * Whether password will be generated on server-side.
         *
         * @return this builder
         */
        public CqlRoleBuilder generatedPassword(boolean generatedPassword)
        {
            this.generatePassword = generatedPassword;
            this.password = null;
            return this;
        }

        /**
         * Set the password for the role
         *
         * @param password the password (will be quoted in output)
         * @return this builder
         */
        public CqlRoleBuilder password(String password)
        {
            this.password = password;
            this.generatePassword = false;
            return this;
        }

        /**
         * Set whether the role can log in
         *
         * @param canLogin true if role can login, false otherwise
         * @return this builder
         */
        public CqlRoleBuilder login(boolean canLogin)
        {
            this.login = canLogin;
            return this;
        }

        /**
         * Set whether the role is a superuser
         *
         * @param isSuperuser true if role is superuser, false otherwise
         * @return this builder
         */
        public CqlRoleBuilder superuser(boolean isSuperuser)
        {
            this.superuser = isSuperuser;
            return this;
        }

        /**
         * Add IF NOT EXISTS clause
         *
         * @return this builder
         */
        public CqlRoleBuilder ifNotExists()
        {
            this.ifNotExists = true;
            return this;
        }

        /**
         * @param roleNameOptions options for role name generation
         * @return this builder
         */
        public CqlRoleBuilder option(Map<String, String> roleNameOptions)
        {
            roleNameOptions.forEach(this::option);
            return this;
        }

        /**
         * Add custom option with string value
         *
         * @param key   option name
         * @param value option value
         * @return this builder
         */
        public CqlRoleBuilder option(String key, String value)
        {
            this.customOptions.put(key, value);
            return this;
        }

        /**
         * Build the final CQL CREATE ROLE statement
         *
         * @return the CQL statement as a string
         */
        public String build()
        {
            if (!generateRole && roleName == null)
                throw new IllegalArgumentException("");

            if (generateRole && ifNotExists)
                throw new IllegalArgumentException("");

            StringBuilder cql = new StringBuilder(generateRole ? "CREATE GENERATED ROLE" : "CREATE ROLE");

            if (!generateRole && ifNotExists)
                cql.append(" IF NOT EXISTS ");

            if (!generateRole)
            {
                if (ifNotExists)
                    cql.append(roleName);
                else
                    cql.append(" ").append(roleName);
            }

            List<String> withOptions = new ArrayList<>();

            if (generatePassword)
                withOptions.add("GENERATED PASSWORD");
            else if (password != null)
                withOptions.add("PASSWORD = '" + password + "'");

            if (login)
                withOptions.add("LOGIN = " + login);

            if (superuser)
                withOptions.add("SUPERUSER = " + superuser);

            if (generateRole && !customOptions.isEmpty())
            {
                withOptions.add("OPTIONS = " + customOptions.entrySet().stream()
                                                            .map(entry -> "'" + entry.getKey() + "':'" + entry.getValue() + "'")
                                                            .collect(Collectors.joining(",", "{", "}")));
            }

            if (!withOptions.isEmpty())
                cql.append(" WITH ").append(String.join(" AND ", withOptions));

            cql.append(";");

            return cql.toString();
        }
    }
}

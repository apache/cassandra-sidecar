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

package org.apache.cassandra.sidecar.configmanagement;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.jetbrains.annotations.NotNull;

/**
 * Validates a list of {@link ConfigurationPatchOperation} instances before application.
 *
 * <p>Validation includes:
 * <ul>
 *   <li>Path format: must start with {@code /configuration/cassandraYaml/} or
 *       {@code /configuration/extraJvmOpts/}</li>
 *   <li>Value presence: required for add/replace/test, must be absent for remove</li>
 *   <li>Duplicate path detection: multiple mutation ops targeting the same path are rejected</li>
 *   <li>Path segments split by {@code /}; empty segments are rejected</li>
 * </ul>
 */
public final class ConfigurationPatchValidator
{
    static final String PATH_PREFIX = "/configuration/";
    static final String CASSANDRA_YAML_SECTION = "cassandraYaml";
    static final String EXTRA_JVM_OPTS_SECTION = "extraJvmOpts";
    static final String CASSANDRA_YAML_PREFIX = PATH_PREFIX + CASSANDRA_YAML_SECTION + "/";
    static final String EXTRA_JVM_OPTS_PREFIX = PATH_PREFIX + EXTRA_JVM_OPTS_SECTION + "/";

    // Allows: -Dproperty.name, -Xmx, -Xss, -XX:+Flag, -XX:-Flag, -XX:Flag
    // Rejects: -javaagent, -agentpath, -agentlib, keys with = or shell metacharacters
    static final Pattern JVM_OPT_KEY_PATTERN = Pattern.compile(
            "^-(D[a-zA-Z][a-zA-Z0-9._-]*|X[a-z][a-zA-Z0-9]*|XX:[+-]?[a-zA-Z][a-zA-Z0-9_]*)$");

    // JVM options that are rejected because they execute arbitrary commands or write to
    // arbitrary filesystem paths. The value pattern permits absolute paths (Cassandra system
    // properties legitimately need them), so path-bearing flags must be blocked by key instead.
    static final Set<String> BLOCKED_JVM_OPTS = Set.of(
            // Execute arbitrary commands
            "-XX:OnOutOfMemoryError",
            "-XX:OnError",
            // Write to arbitrary filesystem paths
            "-XX:ErrorFile",
            "-XX:HeapDumpPath",
            "-XX:LogFile",
            "-XX:FlightRecorderOptions",
            "-XX:StartFlightRecording",
            "-Xloggc",
            "-Xlog",
            "-Xbootclasspath");

    // Allows: alphanumeric, dots, colons, slashes, @, +, commas, hyphens, braces, brackets, quotes (max 512 chars).
    // Quotes/braces/brackets permit JSON values. Whitespace is rejected because the Cassandra launcher
    // word-splits it; shell metacharacters (;|&$`), newlines and other control characters are also rejected.
    static final Pattern JVM_OPT_VALUE_PATTERN = Pattern.compile("^[a-zA-Z0-9._:/@+,\"{}\\[\\]-]{0,512}$");

    // Matches /../ path traversal sequences (start, middle, or end of path)
    static final Pattern PATH_TRAVERSAL_PATTERN = Pattern.compile("(?:^|/)\\.\\.(?:/|$)");

    private ConfigurationPatchValidator()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Validates a list of patch operations and returns their parsed representations.
     *
     * @param operations the raw patch operations to validate
     * @return the list of parsed operations with extracted section, top-level key, and nested segments
     * @throws ConfigurationPatchException if any validation check fails
     */
    @NotNull
    public static List<ParsedPatchOperation> validate(@NotNull List<ConfigurationPatchOperation> operations)
    {
        if (operations.isEmpty())
        {
            throw new ConfigurationPatchException("Patch operations list must not be empty", null);
        }

        Set<String> seenMutationPaths = new HashSet<>();
        List<ParsedPatchOperation> parsed = new ArrayList<>(operations.size());

        for (ConfigurationPatchOperation op : operations)
        {
            validateValuePresence(op);
            ParsedPatchOperation parsedOp = parsePath(op);
            // Only mutation ops (add/remove/replace) are checked for duplicates.
            // test is read-only and may target the same path as a subsequent mutation.
            if (op.op() != ConfigurationPatchOperation.Op.TEST && !seenMutationPaths.add(op.path()))
            {
                throw new ConfigurationPatchException(
                        "Duplicate path in patch: '" + op.path() + "'", op);
            }
            parsed.add(parsedOp);
        }
        return parsed;
    }

    private static void validateValuePresence(ConfigurationPatchOperation op)
    {
        switch (op.op())
        {
            case ADD:
            case REPLACE:
            case TEST:
                if (op.value() == null)
                {
                    throw new ConfigurationPatchException(
                            "Operation '" + op.op() + "' requires a value", op);
                }
                break;
            case REMOVE:
                if (op.value() != null)
                {
                    throw new ConfigurationPatchException(
                            "Operation 'REMOVE' must not have a value", op);
                }
                break;
        }
    }

    private static ParsedPatchOperation parsePath(ConfigurationPatchOperation op)
    {
        String path = op.path();

        if (path.startsWith(CASSANDRA_YAML_PREFIX))
        {
            String remaining = path.substring(CASSANDRA_YAML_PREFIX.length());
            List<String> segments = parsePointerSegments(remaining, op);
            if (segments.isEmpty())
            {
                throw new ConfigurationPatchException(
                        "Path must specify at least one key after section: '" + path + "'", op);
            }
            String topLevelKey = segments.get(0);
            List<String> nestedSegments = segments.size() > 1 ? segments.subList(1, segments.size())
                                                              : List.of();
            return new ParsedPatchOperation(op, CASSANDRA_YAML_SECTION, topLevelKey, nestedSegments);
        }
        else if (path.startsWith(EXTRA_JVM_OPTS_PREFIX))
        {
            String remaining = path.substring(EXTRA_JVM_OPTS_PREFIX.length());
            List<String> segments = parsePointerSegments(remaining, op);
            if (segments.isEmpty())
            {
                throw new ConfigurationPatchException(
                        "Path must specify at least one key after section: '" + path + "'", op);
            }
            if (segments.size() > 1)
            {
                throw new ConfigurationPatchException(
                        "extraJvmOpts paths must be flat (no nested segments): '" + path + "'", op);
            }
            String jvmOptKey = segments.get(0);
            validateJvmOptKey(jvmOptKey, op);
            if (op.value() != null)
            {
                validateJvmOptValue(String.valueOf(op.value()), op);
            }
            return new ParsedPatchOperation(op, EXTRA_JVM_OPTS_SECTION, jvmOptKey, List.of());
        }
        else
        {
            throw new ConfigurationPatchException(
                    "Path must start with '/configuration/cassandraYaml/' or "
                    + "'/configuration/extraJvmOpts/': '" + path + "'", op);
        }
    }

    private static void validateJvmOptKey(String key, ConfigurationPatchOperation op)
    {
        if (!JVM_OPT_KEY_PATTERN.matcher(key).matches())
        {
            throw new ConfigurationPatchException(
                    "Invalid JVM option key '" + key + "': must be a valid JVM option "
                    + "(-Dproperty.name, -Xflag, or -XX:[+-]Flag)", op);
        }
        if (BLOCKED_JVM_OPTS.contains(key))
        {
            throw new ConfigurationPatchException(
                    "Blocked extraJvmOpts key '" + key + "': this JVM option is not allowed", op);
        }
    }

    private static void validateJvmOptValue(String value, ConfigurationPatchOperation op)
    {
        if (!JVM_OPT_VALUE_PATTERN.matcher(value).matches())
        {
            throw new ConfigurationPatchException(
                    "Invalid extraJvmOpts value: contains disallowed characters or exceeds 512 characters", op);
        }
        if (PATH_TRAVERSAL_PATTERN.matcher(value).find())
        {
            throw new ConfigurationPatchException(
                    "Invalid extraJvmOpts value: path traversal ('..') is not allowed", op);
        }
    }

    /**
     * Splits a path remainder into segments by {@code /}.
     */
    static List<String> parsePointerSegments(String pointer, ConfigurationPatchOperation op)
    {
        if (pointer.isEmpty())
        {
            return List.of();
        }

        String[] parts = pointer.split("/", -1);
        List<String> segments = new ArrayList<>(parts.length);
        for (String part : parts)
        {
            if (part.isEmpty())
            {
                throw new ConfigurationPatchException(
                        "Path contains empty segment: '" + op.path() + "'", op);
            }
            segments.add(part);
        }
        return segments;
    }

    /**
     * A validated and parsed patch operation with its path decomposed into section, top-level key,
     * and optional nested segments.
     */
    public static class ParsedPatchOperation
    {
        private final ConfigurationPatchOperation operation;
        private final String section;
        private final String topLevelKey;
        private final List<String> nestedSegments;

        ParsedPatchOperation(@NotNull ConfigurationPatchOperation operation,
                             @NotNull String section,
                             @NotNull String topLevelKey,
                             @NotNull List<String> nestedSegments)
        {
            this.operation = operation;
            this.section = section;
            this.topLevelKey = topLevelKey;
            this.nestedSegments = nestedSegments;
        }

        @NotNull
        public ConfigurationPatchOperation operation()
        {
            return operation;
        }

        /** Either "cassandraYaml" or "extraJvmOpts" */
        @NotNull
        public String section()
        {
            return section;
        }

        /** The first path segment after the section (e.g. "memtable", "concurrent_reads") */
        @NotNull
        public String topLevelKey()
        {
            return topLevelKey;
        }

        /**
         * Segments after the top-level key; empty for flat paths.
         * For example, path {@code /configuration/cassandraYaml/memtable/heap_pool} yields top-level key
         * {@code memtable} and nested segments {@code [heap_pool]}.
         */
        @NotNull
        public List<String> nestedSegments()
        {
            return nestedSegments;
        }

        public boolean isNested()
        {
            return !nestedSegments.isEmpty();
        }
    }
}

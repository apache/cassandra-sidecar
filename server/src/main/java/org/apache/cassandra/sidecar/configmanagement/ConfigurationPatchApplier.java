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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.sidecar.configmanagement.ConfigurationPatchValidator.CASSANDRA_YAML_SECTION;
import static org.apache.cassandra.sidecar.configmanagement.ConfigurationPatchValidator.EXTRA_JVM_OPTS_SECTION;

/**
 * Applies validated patch operations to the configuration overlay.
 *
 * <p>Paths reference the effective configuration (base + overlay merged),
 * but mutations target the overlay only. For nested paths within a top-level {@code cassandraYaml} key,
 * the entire top-level key value is copied from the effective config into the overlay before applying
 * the leaf change (copy-siblings strategy).
 *
 * <p>Because copy-siblings captures a snapshot of the whole top-level block into the overlay, and the
 * overlay is deep-merged over the base ({@link ConfigUtils#mergeConfigurations}), two behaviors follow
 * that callers should be aware of:
 * <ul>
 *   <li><b>Editing a nested leaf pins its sibling leaves against base drift.</b> Every leaf present in
 *       the top-level block at edit time is copied into the overlay and thereafter shadows the base, so
 *       later changes to those same leaves in the base template no longer surface in the effective
 *       configuration. Keys added to the base block <em>after</em> the edit are not pinned - the deep
 *       merge still surfaces them.</li>
 *   <li><b>Removing an overlaid leaf reverts it to the current base value.</b> The leaf is deleted from
 *       the overlay, so the effective value falls back to whatever the base template currently holds,
 *       which may differ from the value that was in effect when the overlay was written.</li>
 * </ul>
 *
 * <p>Operations are applied sequentially (RFC 6902 section 5): each operation is validated and
 * applied against the effective configuration produced by the previous operation, so later
 * operations observe the effects of earlier ones. Mutations target a throwaway copy of the overlay;
 * if any operation fails, the exception propagates and the copy is discarded, so nothing is
 * persisted (all-or-nothing atomicity).
 */
public final class ConfigurationPatchApplier
{
    private ConfigurationPatchApplier()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Applies a list of validated patch operations and returns the updated overlay.
     *
     * <p>The effective configuration is re-derived from {@code baseConfig} and the evolving overlay
     * before each operation, so an operation's preconditions are checked against the state left by the
     * preceding operations rather than the initial snapshot.
     *
     * @param parsedOps      the validated and parsed operations
     * @param baseConfig     the base configuration (base template) the overlay is applied on top of
     * @param currentOverlay the current overlay (may be empty)
     * @return the updated overlay with all operations applied
     * @throws ConfigurationPatchException if any precondition check fails
     */
    @NotNull
    public static CassandraConfigurationOverlay apply(
            @NotNull List<ConfigurationPatchValidator.ParsedPatchOperation> parsedOps,
            @NotNull CassandraConfigurationOverlay baseConfig,
            @NotNull CassandraConfigurationOverlay currentOverlay)
    {
        JsonObject overlayYaml = currentOverlay.cassandraYaml().copy();
        Map<String, String> overlayOpts = new LinkedHashMap<>(currentOverlay.extraJvmOpts());

        for (ConfigurationPatchValidator.ParsedPatchOperation parsed : parsedOps)
        {
            // Re-derive the effective configuration so each op sees the effects of the previous ones.
            JsonObject effectiveYaml = ConfigUtils.mergeConfigurations(baseConfig.cassandraYaml(), overlayYaml);
            Map<String, String> effectiveOpts = ConfigUtils.mergeOpts(baseConfig.extraJvmOpts(), overlayOpts);

            checkPreconditions(parsed, effectiveYaml, effectiveOpts, overlayYaml, overlayOpts);
            applyMutation(parsed, effectiveYaml, overlayYaml, overlayOpts);
        }

        return new CassandraConfigurationOverlay(overlayYaml, overlayOpts);
    }

    private static void checkPreconditions(ConfigurationPatchValidator.ParsedPatchOperation parsed,
                                           JsonObject effectiveYaml,
                                           Map<String, String> effectiveOpts,
                                           JsonObject overlayYaml,
                                           Map<String, String> overlayOpts)
    {
        if (parsed.section().equals(CASSANDRA_YAML_SECTION))
        {
            checkYamlPreconditions(parsed, effectiveYaml, overlayYaml);
        }
        else if (parsed.section().equals(EXTRA_JVM_OPTS_SECTION))
        {
            checkJvmOptsPreconditions(parsed, effectiveOpts, overlayOpts);
        }
    }

    private static void checkYamlPreconditions(ConfigurationPatchValidator.ParsedPatchOperation parsed,
                                               JsonObject effectiveYaml,
                                               JsonObject overlayYaml)
    {
        ConfigurationPatchOperation op = parsed.operation();

        switch (op.op())
        {
            case TEST:
                checkNestedPathTraversable(effectiveYaml, parsed);
                Object actualValue = resolveValue(effectiveYaml, parsed.topLevelKey(), parsed.nestedSegments());
                if (actualValue == null)
                {
                    throw new ConfigurationPatchException(
                            "Test failed: path does not exist in effective config: '" + op.path() + "'", op);
                }
                // Both sides are Vert.x JSON types: resolveValue returns JsonObject/JsonArray/scalars,
                // and op.value() is expected to be read from the request via JsonObject.getValue,
                // which wraps objects/arrays the same way.
                if (!Objects.equals(actualValue, op.value()))
                {
                    throw new ConfigurationPatchException(
                            "Test failed: expected " + op.value() + " but found " + actualValue
                            + " at path '" + op.path() + "'", op);
                }
                break;

            case REPLACE:
                checkNestedPathTraversable(effectiveYaml, parsed);
                Object existingValue = resolveValue(effectiveYaml, parsed.topLevelKey(), parsed.nestedSegments());
                if (existingValue == null)
                {
                    throw new ConfigurationPatchException(
                            "Replace failed: path does not exist in effective config: '" + op.path() + "'", op);
                }
                break;

            case REMOVE:
                checkNestedPathTraversable(overlayYaml, parsed);
                Object overlayValue = resolveValue(overlayYaml, parsed.topLevelKey(), parsed.nestedSegments());
                if (overlayValue == null)
                {
                    throw new ConfigurationPatchException(
                            "Remove failed: path does not exist in overlay (may only exist in base template): '"
                            + op.path() + "'", op);
                }
                break;

            case ADD:
                if (parsed.isNested())
                {
                    // Parent must exist in effective config (RFC 6902)
                    List<String> parentSegments = parsed.nestedSegments()
                                                       .subList(0, parsed.nestedSegments().size() - 1);
                    checkNestedPathTraversable(effectiveYaml, parsed);
                    Object parent = resolveValue(effectiveYaml, parsed.topLevelKey(), parentSegments);
                    if (parent == null && !parentSegments.isEmpty())
                    {
                        throw new ConfigurationPatchException(
                                "Add failed: parent path does not exist in effective config: '" + op.path() + "'", op);
                    }
                    if (parent != null && !(parent instanceof JsonObject))
                    {
                        throw new ConfigurationPatchException(
                                "Add failed: parent is not an object at path: '" + op.path() + "'", op);
                    }
                }
                break;
        }
    }

    /**
     * Validates that the nested path can be traversed without hitting a non-object (e.g., an array)
     * at an intermediate segment. Throws a clear error if a segment resolves to a value that
     * cannot be traversed further.
     */
    private static void checkNestedPathTraversable(JsonObject yaml,
                                                   ConfigurationPatchValidator.ParsedPatchOperation parsed)
    {
        if (!parsed.isNested())
        {
            return;
        }
        ConfigurationPatchOperation op = parsed.operation();
        Object current = yaml.getValue(parsed.topLevelKey());
        if (current == null)
        {
            return; // will be caught by subsequent resolveValue null check
        }

        for (int i = 0; i < parsed.nestedSegments().size() - 1; i++)
        {
            JsonObject currentObj = asJsonObject(current);
            if (currentObj == null)
            {
                throw new ConfigurationPatchException(
                        "Path traversal failed: intermediate value is not an object at path: '"
                        + op.path() + "'", op);
            }
            current = currentObj.getValue(parsed.nestedSegments().get(i));
            if (current == null)
            {
                return; // will be caught by subsequent resolveValue null check
            }
        }
    }

    private static void checkJvmOptsPreconditions(ConfigurationPatchValidator.ParsedPatchOperation parsed,
                                                  Map<String, String> effectiveOpts,
                                                  Map<String, String> overlayOpts)
    {
        ConfigurationPatchOperation op = parsed.operation();
        String key = parsed.topLevelKey();

        switch (op.op())
        {
            case TEST:
                if (!effectiveOpts.containsKey(key))
                {
                    throw new ConfigurationPatchException(
                            "Test failed: key does not exist in effective extraJvmOpts: '" + op.path() + "'", op);
                }
                String actual = effectiveOpts.get(key);
                if (!Objects.equals(actual, op.value()))
                {
                    throw new ConfigurationPatchException(
                            "Test failed: expected '" + op.value() + "' but found '" + actual
                            + "' at path '" + op.path() + "'", op);
                }
                break;

            case REPLACE:
                if (!effectiveOpts.containsKey(key))
                {
                    throw new ConfigurationPatchException(
                            "Replace failed: key does not exist in effective extraJvmOpts: '" + op.path() + "'", op);
                }
                checkNoConflictingBooleanOpt(effectiveOpts, key, op);
                break;

            case REMOVE:
                if (!overlayOpts.containsKey(key))
                {
                    throw new ConfigurationPatchException(
                            "Remove failed: key does not exist in overlay extraJvmOpts "
                            + "(may only exist in base template): '" + op.path() + "'", op);
                }
                break;

            case ADD:
                // extraJvmOpts is flat, so the parent always exists. Only guard against introducing a
                // boolean option that conflicts with one already in the effective configuration
                // (e.g. adding -XX:-UseG1GC while -XX:+UseG1GC is set by the base or a prior op).
                checkNoConflictingBooleanOpt(effectiveOpts, key, op);
                break;
        }
    }

    private static void checkNoConflictingBooleanOpt(Map<String, String> effectiveOpts, String key,
                                                     ConfigurationPatchOperation op)
    {
        if (CassandraConfigurationOverlay.hasConflictingBooleanOpt(effectiveOpts, key))
        {
            String conflicting = CassandraConfigurationOverlay.conflictingBooleanOpt(key);
            throw new ConfigurationPatchException(
                    "Conflicting boolean JVM option: '" + key + "' conflicts with existing '"
                    + conflicting + "' in the effective configuration", op);
        }
    }

    private static void applyMutation(ConfigurationPatchValidator.ParsedPatchOperation parsed,
                                      JsonObject effectiveYaml,
                                      JsonObject updatedYaml,
                                      Map<String, String> updatedOpts)
    {
        ConfigurationPatchOperation op = parsed.operation();

        if (op.op() == ConfigurationPatchOperation.Op.TEST)
        {
            return; // test is read-only
        }

        if (parsed.section().equals(CASSANDRA_YAML_SECTION))
        {
            applyYamlMutation(parsed, effectiveYaml, updatedYaml);
        }
        else if (parsed.section().equals(EXTRA_JVM_OPTS_SECTION))
        {
            applyJvmOptsMutation(parsed, updatedOpts);
        }
    }

    private static void applyYamlMutation(ConfigurationPatchValidator.ParsedPatchOperation parsed,
                                          JsonObject effectiveYaml,
                                          JsonObject updatedYaml)
    {
        ConfigurationPatchOperation op = parsed.operation();

        if (!parsed.isNested())
        {
            // Top-level key: direct put/remove on overlay yaml
            if (op.op() == ConfigurationPatchOperation.Op.REMOVE)
            {
                updatedYaml.remove(parsed.topLevelKey());
            }
            else
            {
                updatedYaml.put(parsed.topLevelKey(), op.value());
            }
        }
        else if (op.op() == ConfigurationPatchOperation.Op.REMOVE)
        {
            // Nested remove: modify the existing overlay subtree in-place.
            // The precondition check already verified the leaf exists in the overlay.
            // updatedYaml is already a deep copy so in-place mutation is safe.
            JsonObject topLevelObject = asJsonObject(updatedYaml.getValue(parsed.topLevelKey()));
            if (topLevelObject != null)
            {
                removeAtPath(topLevelObject, parsed.nestedSegments());
            }
        }
        else
        {
            // Nested add/replace: copy-siblings strategy.
            // If a prior op in this batch already copied the top-level key into the overlay,
            // operate on that copy. Otherwise, copy from effective config to ensure the overlay
            // is self-contained at the top-level key granularity.
            JsonObject topLevelObject;
            if (updatedYaml.containsKey(parsed.topLevelKey()))
            {
                topLevelObject = asJsonObject(updatedYaml.getValue(parsed.topLevelKey()));
            }
            else
            {
                topLevelObject = deepCopyValue(effectiveYaml.getValue(parsed.topLevelKey()));
            }
            if (topLevelObject == null)
            {
                topLevelObject = new JsonObject();
            }
            setAtPath(topLevelObject, parsed.nestedSegments(), op.value());
            updatedYaml.put(parsed.topLevelKey(), topLevelObject);
        }
    }

    private static void applyJvmOptsMutation(ConfigurationPatchValidator.ParsedPatchOperation parsed,
                                             Map<String, String> updatedOpts)
    {
        ConfigurationPatchOperation op = parsed.operation();
        String key = parsed.topLevelKey();

        if (op.op() == ConfigurationPatchOperation.Op.REMOVE)
        {
            updatedOpts.remove(key);
        }
        else
        {
            // Boolean-opt conflicts are rejected by checkJvmOptsPreconditions against the current
            // effective configuration, so the mutation here is unconditional.
            updatedOpts.put(key, String.valueOf(op.value()));
        }
    }

    /**
     * Resolves a value at the given path within a JsonObject.
     *
     * @param root           the root object to traverse
     * @param topLevelKey    the first key to look up
     * @param nestedSegments remaining path segments after the top-level key
     * @return the value at the path, or {@code null} if any segment is missing
     */
    @Nullable
    @SuppressWarnings("unchecked")
    static Object resolveValue(JsonObject root, String topLevelKey, List<String> nestedSegments)
    {
        Object current = root.getValue(topLevelKey);
        if (current == null)
        {
            return null;
        }

        for (String segment : nestedSegments)
        {
            JsonObject obj = asJsonObject(current);
            if (obj == null)
            {
                return null;
            }
            current = obj.getValue(segment);
            if (current == null)
            {
                return null;
            }
        }
        return current;
    }

    private static void setAtPath(JsonObject root, List<String> segments, Object value)
    {
        JsonObject parent = root;
        for (int i = 0; i < segments.size() - 1; i++)
        {
            Object child = parent.getValue(segments.get(i));
            JsonObject childObj = asJsonObject(child);
            if (childObj == null)
            {
                childObj = new JsonObject();
                parent.put(segments.get(i), childObj);
            }
            parent = childObj;
        }
        parent.put(segments.get(segments.size() - 1), value);
    }

    private static void removeAtPath(JsonObject root, List<String> segments)
    {
        JsonObject parent = root;
        for (int i = 0; i < segments.size() - 1; i++)
        {
            Object child = parent.getValue(segments.get(i));
            JsonObject childObj = asJsonObject(child);
            if (childObj == null)
            {
                // Unreachable: the remove precondition already verified the full path exists in the
                // overlay against the current (post-previous-op) state, so every intermediate is present.
                throw new IllegalStateException("Overlay path segment '" + segments.get(i)
                                                + "' is missing or not an object during remove");
            }
            parent = childObj;
        }
        parent.remove(segments.get(segments.size() - 1));
    }

    // Values are always read via JsonObject.getValue, which wraps nested Maps into JsonObject,
    // so a raw Map never reaches here; only JsonObject (or a non-object such as JsonArray/scalar).
    @Nullable
    private static JsonObject asJsonObject(@Nullable Object value)
    {
        return value instanceof JsonObject ? (JsonObject) value : null;
    }

    private static JsonObject deepCopyValue(@Nullable Object value)
    {
        return value instanceof JsonObject ? ((JsonObject) value).copy() : new JsonObject();
    }

}

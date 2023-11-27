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

package org.apache.cassandra.sidecar.routes;

import java.util.Objects;
import java.util.Optional;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import io.vertx.core.Future;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.sidecar.db.RestoreJob;

/**
 * Utils _only_ for routing context.
 */
public class RoutingContextUtils
{
    private RoutingContextUtils() {}

    /**
     * {@link TypedKey} is used to pass information through {@link RoutingContext}. Encapsulates both object value and
     * its type.
     * @param <T> type of value associated with current {@link TypedKey}
     */
    public static class TypedKey<T>
    {
        private final Class<T> type;
        private final String key;

        private TypedKey(Class<T> type, String key)
        {
            this.type = type;
            this.key = key;
        }

        @Override
        public String toString()
        {
            return "TypedKey{type=" + type.getCanonicalName() + ", key=" + key + "}";
        }
    }

    public static final TypedKey<KeyspaceMetadata> SC_KEYSPACE_METADATA = new TypedKey<>(KeyspaceMetadata.class,
                                                                                         "SC_KEYSPACE_METADATA");
    public static final TypedKey<TableMetadata> SC_TABLE_METADATA = new TypedKey<>(TableMetadata.class,
                                                                                   "SC_TABLE_METADATA");
    public static final TypedKey<RestoreJob> SC_RESTORE_JOB = new TypedKey<>(RestoreJob.class,
                                                                             "SC_RESTORE_JOB");
    public static final TypedKey<QualifiedTableName> SC_QUALIFIED_TABLE_NAME
    = new TypedKey<>(QualifiedTableName.class, "SC_QUALIFIED_TABLE_NAME");

    public static <T> void put(RoutingContext context, TypedKey<T> typedKey, T value)
    {
        context.put(typedKey.key, value);
    }

    /**
     * Get the associated value from context according to the typed key
     * @return the associated value
     * @param <T> type of the value, determined by the typed key
     * @throws RoutingContextException when no value can be returned
     */
    public static <T> T get(RoutingContext context, TypedKey<T> typedKey) throws RoutingContextException
    {
        Object obj = context.get(typedKey.key);
        if (obj == null)
        {
            throw new RoutingContextException("No value found for key " + typedKey);
        }

        if (!Objects.equals(obj.getClass(), typedKey.type))
        {
            throw new RoutingContextException("The class of the stored value does not match with key " + typedKey);
        }

        return typedKey.type.cast(obj);
    }

    /**
     * Similar to {@link #get(RoutingContext, TypedKey)}, but wrap the result in {@link Optional}
     * @return an optional of the result
     */
    public static <T> Optional<T> getAsOptional(RoutingContext context, TypedKey<T> typedKey)
    {
        try
        {
            return Optional.of(get(context, typedKey));
        }
        catch (RoutingContextException e)
        {
            return Optional.empty();
        }
    }

    /**
     * Similar to {@link #get(RoutingContext, TypedKey)}, but wrap the result in {@link Future}
     * @return a future of the result
     */
    public static <T> Future<T> getAsFuture(RoutingContext context, TypedKey<T> typedKey)
    {
        return Future.future(promise -> {
            try
            {
                promise.complete(get(context, typedKey));
            }
            catch (RoutingContextException e)
            {
                promise.fail(e);
            }
        });
    }

    /**
     * Exception thrown when handling of {@link TypedKey} value passed through {@link RoutingContext} results in failure
     */
    public static class RoutingContextException extends Exception
    {
        public RoutingContextException(String msg)
        {
            super(msg);
        }

        public RoutingContextException(String msg, Throwable cause)
        {
            super(msg, cause);
        }
    }
}

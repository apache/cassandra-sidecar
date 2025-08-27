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

package org.apache.cassandra.sidecar.handlers.validations;

import com.datastax.driver.core.KeyspaceMetadata;
import io.vertx.core.Future;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

/**
 * Utility class for validation handlers that check the existence of Cassandra schema elements.
 */
public class ValidationUtils
{
    private ValidationUtils()
    {
        // Utility class, not meant to be instantiated
    }

    /**
     * Fetches keyspace metadata from Cassandra for the given host and keyspace name.
     *
     * @param metadataFetcher the metadata fetcher
     * @param executorPools   the executor pools
     * @param host            the host to fetch metadata from
     * @param keyspace        the keyspace name
     * @return a Future containing the KeyspaceMetadata, or null if the keyspace doesn't exist
     */
    public static Future<KeyspaceMetadata> getKeyspaceMetadata(InstanceMetadataFetcher metadataFetcher,
                                                              ExecutorPools executorPools,
                                                              String host,
                                                              String keyspace)
    {
        return executorPools.service().executeBlocking(() -> metadataFetcher.instance(host)
                                                                          .delegate()
                                                                          .metadata()
                                                                          .getKeyspace(keyspace));
    }

    /**
     * Validates that a keyspace exists.
     *
     * @param metadataFetcher the metadata fetcher
     * @param executorPools   the executor pools
     * @param host            the host to validate against
     * @param keyspace        the keyspace name to validate
     * @return a Future that completes with the KeyspaceMetadata if the keyspace exists,
     *         or fails with an error if the keyspace doesn't exist or an error occurs
     */
    public static Future<KeyspaceMetadata> validateKeyspaceExists(InstanceMetadataFetcher metadataFetcher,
                                                                 ExecutorPools executorPools,
                                                                 String host,
                                                                 String keyspace)
    {
        return getKeyspaceMetadata(metadataFetcher, executorPools, host, keyspace)
               .compose(keyspaceMetadata -> {
                   if (keyspaceMetadata == null)
                   {
                       return Future.failedFuture("Keyspace " + keyspace + " was not found");
                   }
                   else
                   {
                       return Future.succeededFuture(keyspaceMetadata);
                   }
               });
    }
}

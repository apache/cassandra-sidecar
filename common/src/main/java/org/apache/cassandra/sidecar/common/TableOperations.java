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

package org.apache.cassandra.sidecar.common;

import java.io.IOException;
import java.util.List;

import org.jetbrains.annotations.NotNull;

/**
 * An interface that defines interactions with the column families inside the Cassandra cluster
 */
public interface TableOperations
{
    /**
     * Load new SSTables from the given {@code directory}
     *
     * @param keyspace         the keyspace in Cassandra
     * @param tableName        the table name in Cassandra
     * @param directory        the directory to the new SSTables
     * @param resetLevel       if the level should be reset to 0 on the new SSTables
     * @param clearRepaired    if repaired info should be wiped from the new SSTables
     * @param verifySSTables   if the new SSTables should be verified that they are not corrupt
     * @param verifyTokens     if the tokens in the new SSTables should be verified that they are owned by the
     *                         current node
     * @param invalidateCaches if row cache should be invalidated for the keys in the new SSTables
     * @param extendedVerify   if we should run an extended verify checking all values in the new SSTables
     * @param copyData         if we should copy data from source paths instead of moving them
     * @return list of failed import directories
     */
    List<String> importNewSSTables(@NotNull String keyspace,
                                   @NotNull String tableName,
                                   @NotNull String directory,
                                   boolean resetLevel,
                                   boolean clearRepaired,
                                   boolean verifySSTables,
                                   boolean verifyTokens,
                                   boolean invalidateCaches,
                                   boolean extendedVerify,
                                   boolean copyData);

    /**
     * Returns a list of data directories for the given {@code table}.
     *
     * @param keyspace the keyspace in Cassandra
     * @param table    the table name in Cassandra
     * @return a list of data paths for the Cassandra table
     * @throws IOException when an error occurs reading the data paths
     */
    List<String> getDataPaths(@NotNull String keyspace, @NotNull String table) throws IOException;
}

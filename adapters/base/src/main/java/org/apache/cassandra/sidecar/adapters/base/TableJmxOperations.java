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

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * An interface that pulls methods from the Cassandra MBean interface for ColumnFamilyStore
 */
public interface TableJmxOperations
{

    /**
     * Load new sstables from the given directory
     *
     * @param srcPaths         the path to the new sstables - if it is an empty set, the data directories will
     *                         be scanned
     * @param resetLevel       if the level should be reset to 0 on the new sstables
     * @param clearRepaired    if repaired info should be wiped from the new sstables
     * @param verifySSTables   if the new sstables should be verified that they are not corrupt
     * @param verifyTokens     if the tokens in the new sstables should be verified that they are owned by the
     *                         current node
     * @param invalidateCaches if row cache should be invalidated for the keys in the new sstables
     * @param extendedVerify   if we should run an extended verify checking all values in the new sstables
     * @param copyData         if we should copy data from source paths instead of moving them
     * @return list of failed import directories
     */
    List<String> importNewSSTables(Set<String> srcPaths,
                                   boolean resetLevel,
                                   boolean clearRepaired,
                                   boolean verifySSTables,
                                   boolean verifyTokens,
                                   boolean invalidateCaches,
                                   boolean extendedVerify,
                                   boolean copyData);

    /**
     * @return a list of data paths for the Cassandra table
     * @throws IOException when an error occurs reading the data paths
     */
    List<String> getDataPaths() throws IOException;
}

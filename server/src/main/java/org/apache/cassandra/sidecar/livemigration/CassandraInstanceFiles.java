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

package org.apache.cassandra.sidecar.livemigration;

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.sidecar.common.response.InstanceFileInfo;
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;

/**
 * Lists files that a Cassandra instance has.
 */
public interface CassandraInstanceFiles
{
    /**
     * Lists files belong to a Cassandra instance after excluding default set files to exclude
     * {@link LiveMigrationConfiguration#filesToExclude()} and default set of directories to exclude
     * {@link LiveMigrationConfiguration#directoriesToExclude()}.
     *
     * @return list of files a Cassandra instance has after excluding default files and folders.
     * @throws IOException when failed to read any file or folder.
     */
    List<InstanceFileInfo> files() throws IOException;
}

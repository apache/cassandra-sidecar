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

package org.apache.cassandra.sidecar.handlers.livemigration;

import java.util.HashMap;
import java.util.Map;

/**
 * Enum for holding different type of directories handled by Live Migration.
 */
public enum LiveMigrationDirType
{
    CDC_RAW_DIR("cdc_raw"),
    COMMIT_LOG_DIR("commitlog"),
    DATA_FILE_DIR("data"),
    HINTS_DIR("hints"),
    LOCAL_SYSTEM_DATA_FILE_DIR("local_system_data"),
    SAVED_CACHES_DIR("saved_caches");

    private static final Map<String, LiveMigrationDirType> DIR_TYPE_MAP = new HashMap<>();

    static
    {
        for (LiveMigrationDirType type : values())
        {
            DIR_TYPE_MAP.put(type.dirType, type);
        }
    }

    public final String dirType;

    LiveMigrationDirType(String dirType)
    {
        this.dirType = dirType;
    }

    public static LiveMigrationDirType find(String dirType)
    {
        return DIR_TYPE_MAP.get(dirType);
    }
}

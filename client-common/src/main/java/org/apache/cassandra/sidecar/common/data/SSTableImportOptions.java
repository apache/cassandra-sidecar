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

package org.apache.cassandra.sidecar.common.data;

import java.util.LinkedHashMap;

/**
 * Options for Cassandra import nodetool command. It is like properties.
 * Supports Json serialization and deserialization.
 * <p>
 * This class is used exclusively in the S3 restore job path ({@code CreateRestoreJobRequestPayload}).
 */
public class SSTableImportOptions extends LinkedHashMap<String, String>
{
    public static final boolean DEFAULT_FAIL_ON_MISSING_INDEX = false;
    public static final boolean DEFAULT_VALIDATE_INDEX_CHECKSUM = false;

    private static final String RESET_LEVEL = "resetLevel";
    private static final String CLEAR_REPAIRED = "clearRepaired";
    private static final String VERIFY_SSTABLES = "verifySSTables";
    private static final String VERIFY_TOKENS = "verifyTokens";
    private static final String INVALIDATE_CACHES = "invalidateCaches";
    private static final String EXTENDED_VERIFY = "extendedVerify";
    private static final String COPY_DATA = "copyData";
    private static final String FAIL_ON_MISSING_INDEX = "failOnMissingIndex";
    private static final String VALIDATE_INDEX_CHECKSUM = "validateIndexChecksum";

    public static SSTableImportOptions defaults()
    {
        return new SSTableImportOptions();
    }

    private SSTableImportOptions()
    {
        put(RESET_LEVEL, Boolean.toString(true));
        put(CLEAR_REPAIRED, Boolean.toString(true));
        put(VERIFY_SSTABLES, Boolean.toString(true));
        put(VERIFY_TOKENS, Boolean.toString(true));
        put(INVALIDATE_CACHES, Boolean.toString(true));
        put(EXTENDED_VERIFY, Boolean.toString(true));
        put(COPY_DATA, Boolean.toString(false)); // note: the default is false
        put(FAIL_ON_MISSING_INDEX, Boolean.toString(DEFAULT_FAIL_ON_MISSING_INDEX));
        put(VALIDATE_INDEX_CHECKSUM, Boolean.toString(DEFAULT_VALIDATE_INDEX_CHECKSUM));
    }

    public SSTableImportOptions resetLevel(boolean enabled)
    {
        put(RESET_LEVEL, Boolean.toString(enabled));
        return this;
    }

    public boolean resetLevel()
    {
        return Boolean.parseBoolean(get(RESET_LEVEL));
    }

    public SSTableImportOptions clearRepaired(boolean enabled)
    {
        put(CLEAR_REPAIRED, Boolean.toString(enabled));
        return this;
    }

    public boolean clearRepaired()
    {
        return Boolean.parseBoolean(get(CLEAR_REPAIRED));
    }

    public SSTableImportOptions verifySSTables(boolean enabled)
    {
        put(VERIFY_SSTABLES, Boolean.toString(enabled));
        return this;
    }

    public boolean verifySSTables()
    {
        return Boolean.parseBoolean(get(VERIFY_SSTABLES));
    }

    public SSTableImportOptions verifyTokens(boolean enabled)
    {
        put(VERIFY_TOKENS, Boolean.toString(enabled));
        return this;
    }

    public boolean verifyTokens()
    {
        return Boolean.parseBoolean(get(VERIFY_TOKENS));
    }

    public SSTableImportOptions invalidateCaches(boolean enabled)
    {
        put(INVALIDATE_CACHES, Boolean.toString(enabled));
        return this;
    }

    public boolean invalidateCaches()
    {
        return Boolean.parseBoolean(get(INVALIDATE_CACHES));
    }

    public SSTableImportOptions extendedVerify(boolean enabled)
    {
        put(EXTENDED_VERIFY, Boolean.toString(enabled));
        return this;
    }

    public boolean extendedVerify()
    {
        return Boolean.parseBoolean(get(EXTENDED_VERIFY));
    }

    public SSTableImportOptions copyData(boolean enabled)
    {
        put(COPY_DATA, Boolean.toString(enabled));
        return this;
    }

    public boolean copyData()
    {
        return Boolean.parseBoolean(get(COPY_DATA));
    }

    public SSTableImportOptions failOnMissingIndex(boolean enabled)
    {
        put(FAIL_ON_MISSING_INDEX, Boolean.toString(enabled));
        return this;
    }

    public boolean failOnMissingIndex()
    {
        return Boolean.parseBoolean(get(FAIL_ON_MISSING_INDEX));
    }

    public SSTableImportOptions validateIndexChecksum(boolean enabled)
    {
        put(VALIDATE_INDEX_CHECKSUM, Boolean.toString(enabled));
        return this;
    }

    public boolean validateIndexChecksum()
    {
        return Boolean.parseBoolean(get(VALIDATE_INDEX_CHECKSUM));
    }
}

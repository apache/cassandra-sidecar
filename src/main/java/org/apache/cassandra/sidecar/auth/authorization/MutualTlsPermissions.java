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

package org.apache.cassandra.sidecar.auth.authorization;

import java.util.EnumSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Enum for the permissions in Sidecar
 */
public enum MutualTlsPermissions
{
    CREATE,
    ALTER,
    DROP,

    // data access
    SELECT, // required for SELECT on a table
    MODIFY, // required for INSERT, UPDATE, DELETE, TRUNCATE on a DataResource.

    // permission management
    AUTHORIZE, // required for GRANT and REVOKE of permissions or roles.

    DESCRIBE, // required on the root-level RoleResource to list all Roles

    // UDF permissions
    EXECUTE,  // required to invoke any user defined function or aggregate

    UNMASK, // required to see masked data

    SELECT_MASKED, // required for SELECT on a table with restictions on masked columns

    // Sidecar specific permissions
    STREAM_SSTABLES,
    LIST_SNAPSHOTS,
    CLEAR_SNAPSHOTS,
    CREATE_SNAPSHOT,
    KEYSPACE_SCHEMA,
    RING,
    UPLOAD_SSTABLE,
    KEYSPACE_TOKEN_MAPPING,
    CLEANUP_SSTABLE,
    GOSSIP_INFO,
    CREATE_RESTORE_JOB,
    CREATE_RESTORE_JOB_SLICES,
    RESTORE_JOB,
    PATCH_RESTORE_JOB,
    ABORT_RESTORE_JOB,
    RESTORE_JOB_PROGRESS; // required to create a snapshot on a given table

    public static final Set<MutualTlsPermissions> ALL =
    Sets.immutableEnumSet(EnumSet.range(MutualTlsPermissions.CREATE, MutualTlsPermissions.ABORT_RESTORE_JOB));
    public static final Set<MutualTlsPermissions> NONE = ImmutableSet.of();
}

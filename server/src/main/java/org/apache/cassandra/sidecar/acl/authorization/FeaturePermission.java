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

package org.apache.cassandra.sidecar.acl.authorization;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.CREATE_RESTORE_JOB;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.CREATE_SNAPSHOT;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.DELETE_RESTORE_JOB;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.DELETE_SNAPSHOT;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.DELETE_STAGED_SSTABLE;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.EDIT_RESTORE_JOB;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.IMPORT_STAGED_SSTABLE;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_GOSSIP;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_RESTORE_JOB;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_RING_KEYSPACE_SCOPED;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_SCHEMA;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_SCHEMA_KEYSPACE_SCOPED;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_SNAPSHOT;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_TOPOLOGY;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.STREAM_SNAPSHOT;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.UPLOAD_STAGED_SSTABLE;
import static org.apache.cassandra.sidecar.acl.authorization.CassandraPermissions.SELECT;

/**
 * Enumerates a list of feature level permissions that Sidecar recognizes and honors.
 */
public enum FeaturePermission
{
    BULK_READ_DIRECT("BULK_READ_DIRECT",
                     READ_RING_KEYSPACE_SCOPED,
                     READ_SCHEMA_KEYSPACE_SCOPED,
                     CREATE_SNAPSHOT,
                     READ_SNAPSHOT,
                     DELETE_SNAPSHOT,
                     STREAM_SNAPSHOT,
                     SELECT),

    BULK_WRITE_DIRECT("BULK_WRITE_DIRECT",
                      READ_SCHEMA_KEYSPACE_SCOPED,
                      READ_GOSSIP,
                      READ_TOPOLOGY,
                      UPLOAD_STAGED_SSTABLE,
                      IMPORT_STAGED_SSTABLE,
                      DELETE_STAGED_SSTABLE),

    BULK_WRITE_S3_COMPAT("BULK_WRITE_S3_COMPAT",
                         READ_SCHEMA,
                         READ_TOPOLOGY,
                         CREATE_RESTORE_JOB,
                         READ_RESTORE_JOB,
                         EDIT_RESTORE_JOB,
                         DELETE_RESTORE_JOB),

    CDC("CDC", BasicPermissions.CDC);

    private static final Map<String, FeaturePermission> NAME_TO_FEATURE_PERMISSION
    = Arrays.stream(values())
            .collect(Collectors.collectingAndThen(Collectors.toMap(Enum::name, Function.identity()), Collections::unmodifiableMap));

    private final CompositePermission permission;

    FeaturePermission(String name, Permission... permissions)
    {
        this.permission
        = new CompositePermission(name, Arrays.stream(permissions).collect(Collectors.toList()));
    }

    public Permission permission()
    {
        return permission;
    }

    public static boolean contains(String name)
    {
        return NAME_TO_FEATURE_PERMISSION.containsKey(name);
    }

    public static Permission fromName(String name)
    {
        FeaturePermission featurePermission = NAME_TO_FEATURE_PERMISSION.get(name);
        return featurePermission != null ? featurePermission.permission() : null;
    }
}

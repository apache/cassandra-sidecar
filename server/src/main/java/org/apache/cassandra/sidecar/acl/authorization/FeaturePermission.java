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
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.CREATE_RESTORE_JOB;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.CREATE_SNAPSHOT;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.DELETE_RESTORE_JOB;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.DELETE_SNAPSHOT;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.DELETE_STAGED_SSTABLE;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.EDIT_RESTORE_JOB;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.IMPORT_STAGED_SSTABLE;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_RESTORE_JOB;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_RING_KEYSPACE_SCOPED;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_SCHEMA_KEYSPACE_SCOPED;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_SNAPSHOT;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_TOPOLOGY;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.STREAM_SNAPSHOT;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.UPLOAD_STAGED_SSTABLE;
import static org.apache.cassandra.sidecar.acl.authorization.CassandraPermissions.MODIFY;
import static org.apache.cassandra.sidecar.acl.authorization.CassandraPermissions.SELECT;
import static org.apache.cassandra.sidecar.acl.authorization.ResourceScopes.TABLE_SCOPE;

/**
 * Enumerates a list of feature level permissions that Sidecar recognizes and honors.
 */
public enum FeaturePermission
{
    ANALYTICS_READ_DIRECT("ANALYTICS:READ_DIRECT",
                          READ_RING_KEYSPACE_SCOPED,
                          READ_SCHEMA_KEYSPACE_SCOPED,
                          CREATE_SNAPSHOT,
                          READ_SNAPSHOT,
                          DELETE_SNAPSHOT,
                          STREAM_SNAPSHOT,
                          new StandardPermission(SELECT.name(), TABLE_SCOPE)),

    ANALYTICS_WRITE_DIRECT("ANALYTICS:WRITE_DIRECT",
                           READ_SCHEMA_KEYSPACE_SCOPED,
                           READ_TOPOLOGY,
                           UPLOAD_STAGED_SSTABLE,
                           IMPORT_STAGED_SSTABLE,
                           DELETE_STAGED_SSTABLE,
                           new StandardPermission(MODIFY.name(), TABLE_SCOPE)),

    ANALYTICS_WRITE_S3_COMPAT("ANALYTICS:WRITE_S3_COMPAT",
                              READ_SCHEMA_KEYSPACE_SCOPED,
                              READ_TOPOLOGY,
                              CREATE_RESTORE_JOB,
                              READ_RESTORE_JOB,
                              EDIT_RESTORE_JOB,
                              DELETE_RESTORE_JOB),

    CDC("CDC", BasicPermissions.CDC);

    public static final List<CompositePermission> ALL_FEATURE_PERMISSIONS
    = Arrays.stream(values()).map(FeaturePermission::permission).collect(Collectors.toList());

    private final CompositePermission permission;

    FeaturePermission(String name, Permission... permissions)
    {
        this.permission
        = new CompositePermission(name, Arrays.stream(permissions).collect(Collectors.toList()));
    }

    public CompositePermission permission()
    {
        return permission;
    }
}

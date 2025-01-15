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

/**
 * Basic permissions are the building blocks of the permission-ing system in Sidecar. It offers more granular
 * permissions on specific targets which are listed here. The majority of Sidecar permissions are represented in
 * format {@code domain}:{@code action}.
 * <p>
 * For example, the SNAPSHOT:CREATE permission, CREATE action is allowed for the SNAPSHOT domain. Sample actions are
 * CREATE, READ, EDIT, DELETE, IMPORT, UPLOAD, START etc.
 * <p>
 * Wildcard permissions are supported with ':' wildcard parts divider and '*' wildcard token to match parts:
 * <p>
 * - SNAPSHOT:* allows CREATE, READ and DELETE of snapshots.
 * - *:CREATE allows CREATE action on all possible targets.
 * - *:* allows all possible permissions for specified resource
 *
 * <p>For feature-level permissions refer to the {@link FeaturePermission} class. Feature-level permissions
 * are composite of individual-level permissions related to corresponding features i.e. BULK_READ permission is a
 * composite of CREATE_SNAPSHOT, DELETE_SNAPSHOT etc.
 */
public class BasicPermissions
{
    // SSTable staging related permissions
    public static final Permission UPLOAD_STAGED_SSTABLE = new WildcardPermission("STAGED_SSTABLE:UPLOAD");
    public static final Permission IMPORT_STAGED_SSTABLE = new WildcardPermission("STAGED_SSTABLE:IMPORT");
    public static final Permission DELETE_STAGED_SSTABLE = new WildcardPermission("STAGED_SSTABLE:DELETE");

    // snapshot related permissions
    public static final Permission CREATE_SNAPSHOT = new WildcardPermission("SNAPSHOT:CREATE");
    public static final Permission READ_SNAPSHOT = new WildcardPermission("SNAPSHOT:READ");
    public static final Permission DELETE_SNAPSHOT = new WildcardPermission("SNAPSHOT:DELETE");
    public static final Permission STREAM_SNAPSHOT = new WildcardPermission("SNAPSHOT:STREAM");

    // restore job related permissions
    public static final Permission CREATE_RESTORE_JOB = new WildcardPermission("RESTORE_JOB:CREATE");
    public static final Permission READ_RESTORE_JOB = new WildcardPermission("RESTORE_JOB:READ");
    public static final Permission EDIT_RESTORE_JOB = new WildcardPermission("RESTORE_JOB:EDIT");
    public static final Permission DELETE_RESTORE_JOB = new WildcardPermission("RESTORE_JOB:DELETE");

    // cdc related permissions
    public static final Permission CDC = new StandardPermission("CDC");

    // sidecar operation related permissions
    public static final Permission READ_OPERATIONAL_JOB = new WildcardPermission("OPERATIONAL_JOB:READ");

    // cassandra cluster related permissions
    public static final Permission READ_SCHEMA = new WildcardPermission("SCHEMA:READ");
    public static final Permission READ_GOSSIP = new WildcardPermission("GOSSIP:READ");
    public static final Permission READ_RING = new WildcardPermission("RING:READ");
    public static final Permission READ_TIME_SKEW = new WildcardPermission("TIME_SKEW:READ");
    public static final Permission READ_NODE_SETTINGS = new WildcardPermission("NODE_SETTINGS:READ");
    public static final Permission READ_TOPOLOGY = new WildcardPermission("TOPOLOGY:READ");
}

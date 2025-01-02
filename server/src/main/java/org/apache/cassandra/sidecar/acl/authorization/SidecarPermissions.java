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
 * Sidecar permissions allowed on specific targets are listed here. Majority of sidecar permissions are represented in
 * format <action_allowed>:<action_target>.
 * <p>
 * Example, with CREATE:SNAPSHOT permission, CREATE action is allowed for SNAPSHOT target. Sample actions are
 * CREATE, VIEW, UPDATE, DELETE, STREAM, IMPORT, UPLOAD, START, ABORT etc.
 * <p>
 * Wildcard permissions are supported with ':' wildcard parts divider and '*' wildcard token to match parts:
 * <p>
 * - *:SNAPSHOT allows CREATE:SNAPSHOT, VIEW:SNAPSHOT and DELETE:SNAPSHOT.
 * - CREATE:* allows CREATE action on all possible targets.
 * - *:* allows all possible permissions for specified resource
 */
public class SidecarPermissions
{
    // cassandra cluster related permissions
    public static final Permission VIEW_CLUSTER = new WildcardPermission("VIEW:CLUSTER");

    // SSTable related permissions
    public static final Permission UPLOAD_SSTABLE = new WildcardPermission("UPLOAD:SSTABLE");
    public static final Permission IMPORT_SSTABLE = new WildcardPermission("IMPORT:SSTABLE");
    public static final Permission STREAM_SSTABLE = new WildcardPermission("STREAM:SSTABLE");

    // Upload related permissions
    public static final Permission DELETE_SSTABLE_UPLOAD = new WildcardPermission("DELETE:SSTABLE_UPLOAD");

    // snapshot related permissions
    public static final Permission CREATE_SNAPSHOT = new WildcardPermission("CREATE:SNAPSHOT");
    public static final Permission VIEW_SNAPSHOT = new WildcardPermission("VIEW:SNAPSHOT");
    public static final Permission DELETE_SNAPSHOT = new WildcardPermission("DELETE:SNAPSHOT");

    // restore related permissions
    public static final Permission CREATE_RESTORE_JOB = new WildcardPermission("CREATE:RESTORE_JOB");
    public static final Permission VIEW_RESTORE_JOB = new WildcardPermission("VIEW:RESTORE_JOB");
    public static final Permission UPDATE_RESTORE_JOB = new WildcardPermission("UPDATE:RESTORE_JOB");
    public static final Permission ABORT_RESTORE_JOB = new WildcardPermission("ABORT:RESTORE_JOB");

    // cdc related permissions
    public static final Permission STREAM_CDC = new WildcardPermission("STREAM:CDC");
    public static final Permission VIEW_CDC = new WildcardPermission("VIEW:CDC");

    // sidecar operation related permissions
    public static final Permission VIEW_TASKS = new WildcardPermission("VIEW:TASKS");

    // cassandra data related actions
    public static final Permission VIEW_SCHEMA = new WildcardPermission("VIEW:SCHEMA");
    public static final Permission VIEW_TOPOLOGY = new WildcardPermission("VIEW:TOPOLOGY");
}

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
 * Sidecar actions allowed on specific targets are listed here. Majority of sidecar actions are represented in
 * format <action_allowed>:<action_target>.
 * <p>
 * Example, with CREATE:SNAPSHOT permission, CREATE action is allowed for SNAPSHOT target. Sample actions are
 * CREATE, VIEW, UPDATE, DELETE, STREAM, IMPORT, UPLOAD, START, ABORT etc.
 * <p>
 * Wildcard actions are supported with ':' wildcard parts divider and '*' wildcard token to match parts:
 * <p>
 * - *:SNAPSHOT allows CREATE:SNAPSHOT, VIEW:SNAPSHOT and DELETE:SNAPSHOT.
 * - CREATE:* allows CREATE action on all possible targets.
 * - *:* allows all possible permissions for specified resource
 */
public class SidecarActions
{
    // cassandra cluster related actions
    public static final Action VIEW_CLUSTER = new WildcardAction("VIEW:CLUSTER");

    // SSTable related actions
    public static final Action UPLOAD_SSTABLE = new WildcardAction("UPLOAD:SSTABLE");
    public static final Action IMPORT_SSTABLE = new WildcardAction("IMPORT:SSTABLE");
    public static final Action STREAM_SSTABLE = new WildcardAction("STREAM:SSTABLE");

    // Upload related actions
    public static final Action DELETE_UPLOAD = new WildcardAction("DELETE:UPLOAD");

    // snapshot related actions
    public static final Action CREATE_SNAPSHOT = new WildcardAction("CREATE:SNAPSHOT");
    public static final Action VIEW_SNAPSHOT = new WildcardAction("VIEW:SNAPSHOT");
    public static final Action DELETE_SNAPSHOT = new WildcardAction("DELETE:SNAPSHOT");

    // restore related actions
    public static final Action CREATE_RESTORE = new WildcardAction("CREATE:RESTORE_JOB");
    public static final Action VIEW_RESTORE = new WildcardAction("VIEW:RESTORE_JOB");
    public static final Action UPDATE_RESTORE = new WildcardAction("UPDATE:RESTORE_JOB");
    public static final Action ABORT_RESTORE = new WildcardAction("ABORT:RESTORE_JOB");

    // cdc related actions
    public static final Action STREAM_CDC = new WildcardAction("STREAM:CDC");
    public static final Action VIEW_CDC = new WildcardAction("VIEW:CDC");

    // sidecar internal actions
    public static final Action VIEW_TASKS = new WildcardAction("VIEW:TASKS");

    // cassandra data related actions
    public static final Action VIEW_SCHEMA = new WildcardAction("VIEW:SCHEMA");
    public static final Action VIEW_TOPOLOGY = new WildcardAction("VIEW:TOPOLOGY");
}

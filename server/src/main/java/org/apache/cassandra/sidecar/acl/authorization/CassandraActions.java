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
 * Cassandra actions allowed.
 */
public class CassandraActions
{
    public static final Action CREATE = new StandardAction("CREATE");
    public static final Action ALTER = new StandardAction("ALTER");
    public static final Action DROP = new StandardAction("DROP");
    public static final Action SELECT = new StandardAction("SELECT");
    public static final Action MODIFY = new StandardAction("MODIFY");
    public static final Action AUTHORIZE = new StandardAction("AUTHORIZE");
    public static final Action DESCRIBE = new StandardAction("DESCRIBE");
    public static final Action EXECUTE = new StandardAction("EXECUTE");
    public static final Action UNMASK = new StandardAction("UNMASK");
    public static final Action SELECT_MASKED = new StandardAction("SELECT_MASKED");
}

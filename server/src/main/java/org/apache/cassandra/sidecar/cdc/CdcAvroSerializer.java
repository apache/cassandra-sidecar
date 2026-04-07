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

package org.apache.cassandra.sidecar.cdc;

import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.cdc.TypeCache;
import org.apache.cassandra.cdc.kafka.AvroGenericRecordSerializer;
import org.apache.cassandra.cdc.schemastore.SchemaStore;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

/**
 * Serializer to convert Cassandra CDC events into Avro GenericRecord objects.
 */
public class CdcAvroSerializer extends AvroGenericRecordSerializer
{
    public CdcAvroSerializer(SchemaStore schemaStore,
                             InstanceMetadataFetcher instanceMetadataFetcher,
                             CassandraBridgeFactory cassandraBridgeFactory)
    {
        super(schemaStore, key ->
                           TypeCache.get(cassandraBridgeFactory
                                         .get(instanceMetadataFetcher.callOnFirstAvailableInstance(
                                         instance-> instance.delegate().nodeSettings()).releaseVersion()).getVersion())
                                    .getType(key.keyspace, key.type), "org.apache.cassandra");
    }
}

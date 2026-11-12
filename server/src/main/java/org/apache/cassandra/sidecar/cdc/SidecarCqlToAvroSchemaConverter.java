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


import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.avro.Schema;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CdcBridgeFactory;
import org.apache.cassandra.cdc.avro.CqlToAvroSchemaConverter;
import org.apache.cassandra.spark.data.CqlTable;

/**
 * Class to convert CQL schemas into Avro schemas
 */
public class SidecarCqlToAvroSchemaConverter implements CqlToAvroSchemaConverter
{
    private final Provider<CassandraBridge> cassandraBridgeProvider;


    @Inject
    public SidecarCqlToAvroSchemaConverter(Provider<CassandraBridge> cassandraBridgeProvider)
    {
        this.cassandraBridgeProvider = cassandraBridgeProvider;
    }

    public CassandraBridge cassandraBridge()
    {
        return cassandraBridgeProvider.get();
    }


    public Schema convert(CqlTable cqlTable)
    {
        return CdcBridgeFactory.getCqlToAvroSchemaConverter(cassandraBridge()).convert(cqlTable);
    }
}

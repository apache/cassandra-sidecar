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

package org.apache.cassandra.sidecar.datahub;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.data.template.SetMode;
import com.linkedin.schema.ArrayType;
import com.linkedin.schema.BooleanType;
import com.linkedin.schema.BytesType;
import com.linkedin.schema.DateType;
import com.linkedin.schema.MapType;
import com.linkedin.schema.NullType;
import com.linkedin.schema.NumberType;
import com.linkedin.schema.OtherSchema;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.StringType;
import com.linkedin.schema.TimeType;
import datahub.event.MetadataChangeProposalWrapper;
import org.jetbrains.annotations.NotNull;

/**
 * Converter class for preparing the Schema Metadata aspect for a given Cassandra table
 */
public class TableToSchemaMetadataConverter extends TableToAspectConverter<SchemaMetadata>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TableToSchemaMetadataConverter.class);

    protected static final long VERSION = 1L;

    protected static final SchemaFieldDataType.Type ARRAY   = SchemaFieldDataType.Type.create(new ArrayType());
    protected static final SchemaFieldDataType.Type BOOLEAN = SchemaFieldDataType.Type.create(new BooleanType());
    protected static final SchemaFieldDataType.Type BYTES   = SchemaFieldDataType.Type.create(new BytesType());
    protected static final SchemaFieldDataType.Type DATE    = SchemaFieldDataType.Type.create(new DateType());
    protected static final SchemaFieldDataType.Type MAP     = SchemaFieldDataType.Type.create(new MapType());
    protected static final SchemaFieldDataType.Type NULL    = SchemaFieldDataType.Type.create(new NullType());
    protected static final SchemaFieldDataType.Type NUMBER  = SchemaFieldDataType.Type.create(new NumberType());
    protected static final SchemaFieldDataType.Type STRING  = SchemaFieldDataType.Type.create(new StringType());
    protected static final SchemaFieldDataType.Type TIME    = SchemaFieldDataType.Type.create(new TimeType());

    protected static final Map<String, SchemaFieldDataType.Type> PRIMITIVE_TYPES = new ImmutableMap.Builder<String, SchemaFieldDataType.Type>()
            .put("ascii",     STRING)
            .put("bigint",    NUMBER)
            .put("blob",      BYTES)
            .put("boolean",   BOOLEAN)
            .put("counter",   NUMBER)
            .put("date",      DATE)
            .put("decimal",   NUMBER)
            .put("double",    NUMBER)
            .put("float",     NUMBER)
            .put("inet",      STRING)
            .put("int",       NUMBER)
            .put("smallint",  NUMBER)
            .put("text",      STRING)
            .put("time",      TIME)
            .put("timestamp", DATE)
            .put("timeuuid",  STRING)
            .put("tinyint",   NUMBER)
            .put("uuid",      STRING)
            .put("varchar",   STRING)
            .put("varint",    NUMBER)
            .build();

    public TableToSchemaMetadataConverter(@NotNull IdentifiersProvider identifiers)
    {
        super(identifiers);
    }

    @Override
    @NotNull
    public MetadataChangeProposalWrapper<SchemaMetadata> convert(@NotNull KeyspaceMetadata keyspace,
                                                                 @NotNull TableMetadata table)
    {
        String urn = identifiers.urnDataset(table);

        SchemaFieldArray fields = new SchemaFieldArray();
        table.getColumns().values().stream()
                .flatMap(c -> convertColumn(table, c))
                .forEach(fields::add);

        // Use {@code CREATE TABLE} CQL statement with all associated indexes and views but without
        // UDTs as the native schema; using {@code asCQLQuery()} does not allow formatting produced CQL
        String cql = table.describeWithChildren(true);
        SchemaMetadata.PlatformSchema schema = new SchemaMetadata.PlatformSchema();
        schema.setOtherSchema(new OtherSchema().setRawSchema(cql));
        String hash = DigestUtils.sha1Hex(cql);

        SchemaMetadata aspect = new SchemaMetadata()
                .setSchemaName(table.getName().asInternal())
                .setPlatform(new DataPlatformUrn(identifiers.urnDataPlatform()))
                .setVersion(VERSION)
                .setFields(fields)
                .setPlatformSchema(schema)
                .setHash(hash);

        return wrap(urn, aspect);
    }

    /**
     * Protected method for converting metadata of a single Cassandra column
     * into a non-empty {@link Stream} of DataHub schema field definitions
     *
     * @param column metadata of a single Cassandra column
     * @return non-empty {@link Stream} of DataHub schema field definitions
     */
    @NotNull
    protected Stream<SchemaField> convertColumn(@NotNull TableMetadata table, @NotNull ColumnMetadata column)
    {
        DataType type = column.getType();
        boolean partition = table.getPartitionKey().contains(column);
        boolean key = partition || table.getClusteringColumns().containsKey(column);  // Only check clustering key if needed

        return convertType(column.getName().asInternal(), type, partition, key);
    }

    /**
     * Protected method for converting a single Cassandra data type
     * into a non-empty {@link Stream} of DataHub schema field definitions
     *
     * @param name name of the field
     * @param type type of the field
     * @param partition whether the field is a part of partition key
     * @param key whether the field is a part of any key
     * @return non-empty {@link Stream} of DataHub schema field definitions
     */
    @NotNull
    protected Stream<SchemaField> convertType(@NotNull String name,
                                              @NotNull DataType type,
                                              boolean partition,
                                              boolean key)
    {
        if (type instanceof UserDefinedType)
        {
            UserDefinedType udt = (UserDefinedType) type;
            List<Stream<SchemaField>> streams = new ArrayList<>(udt.getFieldNames().size());
            for (int i = 0; i < udt.getFieldNames().size(); i++)
            {
                Stream<SchemaField> subFields = convertType(name + DELIMITER + udt.getFieldNames().get(i),
                                                            udt.getFieldTypes().get(i), partition, key);
                streams.add(subFields);
            }
            return streams.stream().flatMap(f -> f);
        }
        else
        {
            SchemaFieldDataType datahubType = convertType(type);
            String description = datahubType.getType().isNullType()
                    ? "Unknown Cassandra data type " + type.asCql(true, true).toLowerCase()
                    : null;  // Column-level comments are not supported by Cassandra

            return Stream.of(new SchemaField()
                    .setFieldPath(name)
                    .setNullable(!partition)  // Everything is potentially nullable in Cassandra except for the partition key
                    .setDescription(description, SetMode.REMOVE_IF_NULL)
                    .setType(datahubType)
                    .setNativeDataType(type.asCql(true, true).toLowerCase())
                    .setIsPartitioningKey(partition)
                    .setIsPartOfKey(key));
        }
    }

    /**
     * Protected method for converting data types used by Cassandra into the ones recognized by DataHub,
     * uses {@code NullType} to indicate an unknown or unsupported data type
     *
     * @param cassandraType Cassandra data type
     * @return DataHub data type, or {@code NullType} if unknown/unsupported
     */
    @NotNull
    protected SchemaFieldDataType convertType(@NotNull DataType cassandraType)
    {
        SchemaFieldDataType.Type datahubType = null;
        if (cassandraType instanceof ListType
            || cassandraType instanceof TupleType
            || cassandraType instanceof SetType)
        {
            datahubType = ARRAY;
        }
        else if (cassandraType instanceof com.datastax.oss.driver.api.core.type.MapType)
        {
            datahubType = MAP;
        }
        else
        {
            datahubType = PRIMITIVE_TYPES.get(cassandraType.asCql(false, false));
        }

        if (datahubType == null)
        {
            datahubType = NULL;  // Use the null type as an indicator of an unknown data type
            LOGGER.error("Encountered an unknown data type " + cassandraType);
        }

        return new SchemaFieldDataType()
                .setType(datahubType);
    }
}

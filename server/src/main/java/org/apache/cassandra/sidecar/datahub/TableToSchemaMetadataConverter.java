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

import java.util.Map;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.codec.digest.DigestUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UserType;
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

    protected static final SchemaFieldDataType.Type ARRAY = SchemaFieldDataType.Type.create(new ArrayType());
    protected static final SchemaFieldDataType.Type BOOLEAN = SchemaFieldDataType.Type.create(new BooleanType());
    protected static final SchemaFieldDataType.Type BYTES = SchemaFieldDataType.Type.create(new BytesType());
    protected static final SchemaFieldDataType.Type DATE = SchemaFieldDataType.Type.create(new DateType());
    protected static final SchemaFieldDataType.Type MAP = SchemaFieldDataType.Type.create(new MapType());
    protected static final SchemaFieldDataType.Type NULL = SchemaFieldDataType.Type.create(new NullType());
    protected static final SchemaFieldDataType.Type NUMBER = SchemaFieldDataType.Type.create(new NumberType());
    protected static final SchemaFieldDataType.Type STRING = SchemaFieldDataType.Type.create(new StringType());
    protected static final SchemaFieldDataType.Type TIME = SchemaFieldDataType.Type.create(new TimeType());

    protected static final Map<DataType.Name, SchemaFieldDataType.Type> TYPES =
                                                                              new ImmutableMap.Builder<DataType.Name, SchemaFieldDataType.Type>().put(
                                                                                      DataType.Name.ASCII, STRING)
                                                                                                                                                 .put(DataType.Name.BIGINT,
                                                                                                                                                         NUMBER)
                                                                                                                                                 .put(DataType.Name.BLOB,
                                                                                                                                                         BYTES)
                                                                                                                                                 .put(DataType.Name.BOOLEAN,
                                                                                                                                                         BOOLEAN)
                                                                                                                                                 .put(DataType.Name.COUNTER,
                                                                                                                                                         NUMBER)
                                                                                                                                                 .put(DataType.Name.DATE,
                                                                                                                                                         DATE)
                                                                                                                                                 .put(DataType.Name.DECIMAL,
                                                                                                                                                         NUMBER)
                                                                                                                                                 .put(DataType.Name.DOUBLE,
                                                                                                                                                         NUMBER)
                                                                                                                                                 .put(DataType.Name.FLOAT,
                                                                                                                                                         NUMBER)
                                                                                                                                                 .put(DataType.Name.INET,
                                                                                                                                                         STRING)
                                                                                                                                                 .put(DataType.Name.INT,
                                                                                                                                                         NUMBER)
                                                                                                                                                 .put(DataType.Name.LIST,
                                                                                                                                                         ARRAY)
                                                                                                                                                 .put(DataType.Name.MAP,
                                                                                                                                                         MAP)
                                                                                                                                                 .put(DataType.Name.SET,
                                                                                                                                                         ARRAY)
                                                                                                                                                 .put(DataType.Name.SMALLINT,
                                                                                                                                                         NUMBER)
                                                                                                                                                 .put(DataType.Name.TEXT,
                                                                                                                                                         STRING)
                                                                                                                                                 .put(DataType.Name.TIME,
                                                                                                                                                         TIME)
                                                                                                                                                 .put(DataType.Name.TIMESTAMP,
                                                                                                                                                         DATE)
                                                                                                                                                 .put(DataType.Name.TIMEUUID,
                                                                                                                                                         STRING)
                                                                                                                                                 .put(DataType.Name.TINYINT,
                                                                                                                                                         NUMBER)
                                                                                                                                                 .put(DataType.Name.TUPLE,
                                                                                                                                                         ARRAY)
                                                                                                                                                 .put(DataType.Name.UUID,
                                                                                                                                                         STRING)
                                                                                                                                                 .put(DataType.Name.VARCHAR,
                                                                                                                                                         STRING)
                                                                                                                                                 .put(DataType.Name.VARINT,
                                                                                                                                                         NUMBER)
                                                                                                                                                 .build();

    public TableToSchemaMetadataConverter(@NotNull IdentifiersProvider identifiers)
    {
        super(identifiers);
    }

    @Override
    @NotNull
    public MetadataChangeProposalWrapper<SchemaMetadata> convert(@NotNull TableMetadata table)
    {
        String urn = identifiers.urnDataset(table);

        SchemaFieldArray fields = new SchemaFieldArray();
        table.getColumns()
             .stream()
             .flatMap(this::convertColumn)
             .forEach(fields::add);

        // Use {@code CREATE TABLE} CQL statement with all associated indexes and views but without
        // UDTs as the native schema; using {@code asCQLQuery()} does not allow formatting produced CQL
        String cql = table.exportAsString();
        SchemaMetadata.PlatformSchema schema = new SchemaMetadata.PlatformSchema();
        schema.setOtherSchema(new OtherSchema().setRawSchema(cql));
        String hash = DigestUtils.sha1Hex(cql);

        SchemaMetadata aspect = new SchemaMetadata().setSchemaName(table.getName())
                                                    .setPlatform(new DataPlatformUrn(identifiers.urnDataPlatform()))
                                                    .setVersion(VERSION)
                                                    .setFields(fields)
                                                    .setPlatformSchema(schema)
                                                    .setHash(hash);

        return wrap(urn, aspect);
    }

    /**
     * Protected method for converting metadata of a single Cassandra column into a non-empty {@link Stream} of DataHub schema field definitions
     *
     * @param column metadata of a single Cassandra column
     * @return non-empty {@link Stream} of DataHub schema field definitions
     */
    @NotNull
    protected Stream<SchemaField> convertColumn(@NotNull ColumnMetadata column)
    {
        DataType type = column.getType();
        AbstractTableMetadata table = column.getParent();
        boolean partition = table.getPartitionKey()
                                 .contains(column);
        boolean key = partition || table.getClusteringColumns()
                                        .contains(column); // Only check clustering key if needed

        return convertType(column.getName(), type, partition, key);
    }

    /**
     * Protected method for converting a single Cassandra data type into a non-empty {@link Stream} of DataHub schema field definitions
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
        if (type instanceof UserType)
        {
            UserType udt = (UserType) type;

            return udt.getFieldNames()
                      .stream()
                      .flatMap(field -> convertType(name + DELIMITER + field, udt.getFieldType(field), partition, key));
        }
        else
        {
            DataType.Name cassandraType = type.getName();
            SchemaFieldDataType datahubType = convertType(cassandraType);
            String description = datahubType.getType()
                                            .isNullType() ? "Unknown Cassandra data type " + cassandraType : null; // Column-level comments are not supported by
                                                                                                                   // Cassandra

            return Stream.of(new SchemaField().setFieldPath(name)
                                              .setNullable(!partition) // Everything is potentially nullable in Cassandra except for the partition key
                                              .setDescription(description, SetMode.REMOVE_IF_NULL)
                                              .setType(datahubType)
                                              .setNativeDataType(cassandraType.toString()
                                                                              .toLowerCase())
                                              .setIsPartitioningKey(partition)
                                              .setIsPartOfKey(key));
        }
    }

    /**
     * Protected method for converting data types used by Cassandra into the ones recognized by DataHub, uses {@code NullType} to indicate an unknown or
     * unsupported data type
     *
     * @param cassandraType Cassandra data type
     * @return DataHub data type, or {@code NullType} if unknown/unsupported
     */
    @NotNull
    protected SchemaFieldDataType convertType(@NotNull DataType.Name cassandraType)
    {
        SchemaFieldDataType.Type datahubType = TYPES.get(cassandraType);
        if (datahubType == null)
        {
            datahubType = NULL; // Use the null type as an indicator of an unknown data type
            LOGGER.error("Encountered an unknown data type " + cassandraType);
        }

        return new SchemaFieldDataType().setType(datahubType);
    }
}
